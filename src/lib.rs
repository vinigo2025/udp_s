pub mod client;
pub mod reliable_udp;

use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use reliable_udp::ReliableUdp;

const DEFAULT_UDP_PORT: u16 = 12808;
const DEFAULT_TCP_PORT: u16 = 12809;
const MAX_UDP_PACKET_SIZE: usize = 1250;
const ACK_TIMEOUT: Duration = Duration::from_millis(250);
const ASSEMBLY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 8;

pub fn run_from_args() -> io::Result<()> {
    let (udp_port, tcp_port) = parse_ports(env::args())?;
    run_bridge(udp_port, tcp_port)
}

fn parse_ports(args: impl IntoIterator<Item = String>) -> io::Result<(u16, u16)> {
    let argv: Vec<String> = args.into_iter().collect();

    match argv.len() {
        1 => Ok((DEFAULT_UDP_PORT, DEFAULT_TCP_PORT)),
        3 => {
            let udp_port = parse_port(&argv[1], "udp_port")?;
            let tcp_port = parse_port(&argv[2], "tcp_port")?;
            Ok((udp_port, tcp_port))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {} [udp_port tcp_port]", argv[0]),
        )),
    }
}

fn parse_port(raw: &str, name: &str) -> io::Result<u16> {
    raw.parse::<u16>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {name}: {raw}"),
        )
    })
}

pub fn run_bridge(udp_port: u16, tcp_port: u16) -> io::Result<()> {
    let udp_bind = format!("0.0.0.0:{udp_port}");
    let tcp_target = format!("127.0.0.1:{tcp_port}");
    let udp = UdpSocket::bind(&udp_bind)?;
    let reliable_udp = ReliableUdp::from_socket(
        udp,
        MAX_UDP_PACKET_SIZE,
        ACK_TIMEOUT,
        MAX_RETRIES,
        ASSEMBLY_TIMEOUT,
    )?;

    println!("udp listen: {udp_bind}");
    println!("tcp target: {tcp_target}");

    let last_peer = Arc::new(Mutex::new(None::<SocketAddr>));

    loop {
        let mut tcp = match TcpStream::connect(&tcp_target) {
            Ok(stream) => {
                println!("tcp connected: {tcp_target}");
                stream
            }
            Err(err) => {
                eprintln!("tcp connect error: {err}; retry in 1s");
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(Duration::from_millis(100)))?;
        let mut buf = [0_u8; 64 * 1024];

        'connected: loop {
            loop {
                match reliable_udp.try_recv()? {
                    Some((peer, message)) => {
                        {
                            let mut guard = last_peer
                                .lock()
                                .map_err(|_| io::Error::other("last_peer mutex poisoned"))?;
                            *guard = Some(peer);
                        }
                        if let Err(err) = tcp.write_all(&message) {
                            eprintln!("tcp write error: {err}; reconnecting");
                            break 'connected;
                        }
                    }
                    None => break,
                }
            }

            let n = match tcp.read(&mut buf) {
                Ok(n) => n,
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(err) => {
                    eprintln!("tcp read error: {err}; reconnecting");
                    break 'connected;
                }
            };
            if n == 0 {
                eprintln!("tcp closed by peer; reconnecting");
                break 'connected;
            }

            let peer = {
                let guard = last_peer
                    .lock()
                    .map_err(|_| io::Error::other("last_peer mutex poisoned"))?;
                *guard
            };
            if let Some(peer_addr) = peer {
                if let Err(err) = reliable_udp.send_message(peer_addr, &buf[..n]) {
                    eprintln!("udp send error: {err}");
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}
