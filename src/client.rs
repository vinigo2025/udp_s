use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::thread;
use std::time::Duration;

use crate::reliable_udp::ReliableUdp;

const DEFAULT_TCP_PORT: u16 = 12809;
const DEFAULT_UDP_PORT: u16 = 12808;
const MAX_UDP_PACKET_SIZE: usize = 1250;
const ACK_TIMEOUT: Duration = Duration::from_millis(250);
const ASSEMBLY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 8;

pub fn run_from_args() -> io::Result<()> {
    let (tcp_port, udp_port) = parse_ports(env::args())?;
    run_bridge(tcp_port, udp_port)
}

fn parse_ports(args: impl IntoIterator<Item = String>) -> io::Result<(u16, u16)> {
    let argv: Vec<String> = args.into_iter().collect();
    match argv.len() {
        1 => Ok((DEFAULT_TCP_PORT, DEFAULT_UDP_PORT)),
        3 => {
            let tcp_port = parse_port(&argv[1], "tcp_port")?;
            let udp_port = parse_port(&argv[2], "udp_port")?;
            Ok((tcp_port, udp_port))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {} [tcp_port udp_port]", argv[0]),
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

pub fn run_bridge(tcp_port: u16, udp_port: u16) -> io::Result<()> {
    let tcp_bind = format!("0.0.0.0:{tcp_port}");
    let udp_target: SocketAddr = format!("127.0.0.1:{udp_port}")
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid udp target"))?;

    let listener = TcpListener::bind(&tcp_bind)?;
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let reliable_udp = ReliableUdp::from_socket(
        udp,
        MAX_UDP_PACKET_SIZE,
        ACK_TIMEOUT,
        MAX_RETRIES,
        ASSEMBLY_TIMEOUT,
    )?;

    println!("tcp listen: {tcp_bind}");
    println!("udp target: {udp_target}");

    loop {
        let (mut tcp, peer) = listener.accept()?;
        println!("tcp connected: {peer}");
        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(Duration::from_millis(100)))?;
        let mut buf = [0_u8; 64 * 1024];

        'connected: loop {
            loop {
                match reliable_udp.try_recv()? {
                    Some((src, message)) => {
                        if src != udp_target {
                            continue;
                        }
                        if let Err(err) = tcp.write_all(&message) {
                            eprintln!("tcp write error: {err}; waiting for reconnect");
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
                    eprintln!("tcp read error: {err}; waiting for reconnect");
                    break 'connected;
                }
            };
            if n == 0 {
                eprintln!("tcp closed by peer; waiting for reconnect");
                break 'connected;
            }

            if let Err(err) = reliable_udp.send_message(udp_target, &buf[..n]) {
                eprintln!("udp send error: {err}");
            }
        }
        thread::sleep(Duration::from_millis(300));
    }
}
