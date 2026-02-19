use std::net::{UdpSocket};
use std::io;

pub fn init() -> io::Result<()> {
    // bind port
    let socket = UdpSocket::bind("0.0.0.0:12808")?;
    println!("listen: udp port 12808");

    let mut buf = [0; 65536];

    loop {
        match socket.recv_from(&mut buf) {
            Ok((bytes_received, client_addr)) => {
                println!(
                    "Received {} bytes from {}",
                    bytes_received,
                    client_addr );

                // Send back
                match socket.send_to(&buf[..bytes_received], client_addr) {
                    Ok(_) => {
                        println!("sent back"); },
                    Err(e) => eprintln!("Err send: {}", e),
                } },
            Err(e) => eprintln!("Err recv: {}", e),
        }
    }
}
