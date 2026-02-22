use std::env;
use std::io::{self, Read, Write};
use std::net::{TcpListener, UdpSocket};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const DEFAULT_TCP_PORT: u16 = 12809;
const DEFAULT_UDP_PORT: u16 = 12808;

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
    let udp_target = format!("127.0.0.1:{udp_port}");

    let listener = TcpListener::bind(&tcp_bind)?;
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    udp.connect(&udp_target)?;
    udp.set_read_timeout(Some(Duration::from_millis(200)))?;

    println!("tcp listen: {tcp_bind}");
    println!("udp target: {udp_target}");

    let udp_rx = udp.try_clone()?;
    let udp_tx = udp.try_clone()?;
    let (udp_to_tcp_tx, udp_to_tcp_rx) = mpsc::channel::<Vec<u8>>();

    thread::spawn(move || -> io::Result<()> {
        let mut buf = [0_u8; 65_507];
        loop {
            match udp_rx.recv(&mut buf) {
                Ok(n) => {
                    udp_to_tcp_tx.send(buf[..n].to_vec()).map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "udp->tcp queue closed")
                    })?;
                }
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => return Err(err),
            }
        }
    });

    loop {
        let (mut tcp, peer) = listener.accept()?;
        println!("tcp connected: {peer}");
        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(Duration::from_millis(200)))?;
        let mut buf = [0_u8; 65_507];

        'connected: loop {
            loop {
                match udp_to_tcp_rx.try_recv() {
                    Ok(packet) => {
                        if let Err(err) = tcp.write_all(&packet) {
                            eprintln!("tcp write error: {err}; waiting for reconnect");
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
                    eprintln!("tcp read error: {err}; waiting for reconnect");
                    break 'connected;
                }
            };

            if n == 0 {
                eprintln!("tcp closed by peer; waiting for reconnect");
                break 'connected;
            }

            udp_tx.send(&buf[..n])?;
        }
    }
}
