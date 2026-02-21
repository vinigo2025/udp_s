use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::mpsc;
use std::thread;

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
    let tcp = TcpStream::connect(&tcp_target)?;
    tcp.set_nodelay(true)?;

    println!("udp listen: {udp_bind}");
    println!("tcp target: {tcp_target}");

    let udp_rx = udp.try_clone()?;
    let udp_tx = udp.try_clone()?;

    let mut tcp_tx = tcp.try_clone()?;
    let mut tcp_rx = tcp;

    let (client_tx, client_rx) = mpsc::channel::<SocketAddr>();

    let udp_to_tcp = thread::spawn(move || -> io::Result<()> {
        let mut buf = [0_u8; 65_507];
        loop {
            let (n, src) = udp_rx.recv_from(&mut buf)?;
            client_tx.send(src).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "client address channel closed")
            })?;
            tcp_tx.write_all(&buf[..n])?;
        }
    });

    let tcp_to_udp = thread::spawn(move || -> io::Result<()> {
        let mut last_client: Option<SocketAddr> = None;
        let mut buf = [0_u8; 65_507];

        loop {
            match client_rx.try_recv() {
                Ok(addr) => last_client = Some(addr),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "client address channel disconnected",
                    ));
                }
            }

            let n = tcp_rx.read(&mut buf)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "tcp connection closed",
                ));
            }

            if let Some(addr) = last_client {
                udp_tx.send_to(&buf[..n], addr)?;
            }
        }
    });

    join_bridge_thread(udp_to_tcp)?;
    join_bridge_thread(tcp_to_udp)?;
    Ok(())
}

fn join_bridge_thread(handle: thread::JoinHandle<io::Result<()>>) -> io::Result<()> {
    match handle.join() {
        Ok(res) => res,
        Err(_) => Err(io::Error::other("bridge thread panicked")),
    }
}
