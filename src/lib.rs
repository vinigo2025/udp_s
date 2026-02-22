pub mod client;

use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

const DEFAULT_UDP_PORT: u16 = 12808;
const DEFAULT_TCP_PORT: u16 = 12809;

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
    println!("udp listen: {udp_bind}");
    println!("tcp target: {tcp_target}");

    let udp_rx = udp.try_clone()?;
    let udp_tx = udp.try_clone()?;
    let last_client = Arc::new(Mutex::new(None::<SocketAddr>));
    let last_client_udp = Arc::clone(&last_client);
    let (udp_to_tcp_tx, udp_to_tcp_rx) = mpsc::channel::<Vec<u8>>();

    thread::spawn(move || -> io::Result<()> {
        let mut buf = [0_u8; 65_507];
        loop {
            let (n, src) = udp_rx.recv_from(&mut buf)?;
            {
                let mut guard = last_client_udp
                    .lock()
                    .map_err(|_| io::Error::other("last_client mutex poisoned"))?;
                *guard = Some(src);
            }
            udp_to_tcp_tx.send(buf[..n].to_vec()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "udp->tcp queue closed",
                )
            })?;
        }
    });

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
        tcp.set_read_timeout(Some(Duration::from_millis(200)))?;
        let mut buf = [0_u8; 65_507];

        'connected: loop {
            loop {
                match udp_to_tcp_rx.try_recv() {
                    Ok(packet) => {
                        if let Err(err) = tcp.write_all(&packet) {
                            eprintln!("tcp write error: {err}; reconnecting");
                            break 'connected;
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "udp->tcp queue disconnected",
                        ));
                    }
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
            let addr_opt = {
                let guard = last_client
                    .lock()
                    .map_err(|_| io::Error::other("last_client mutex poisoned"))?;
                *guard
            };
            if let Some(addr) = addr_opt {
                udp_tx.send_to(&buf[..n], addr)?;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}
