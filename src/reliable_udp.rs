use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const MAGIC_0: u8 = 0x55;
const MAGIC_1: u8 = 0x42;
const VERSION: u8 = 1;

const TYPE_DATA: u8 = 1;
const TYPE_ACK: u8 = 2;
const TYPE_NACK: u8 = 3;

const HEADER_LEN: usize = 14;

#[derive(Clone, Copy, PartialEq, Eq)]
enum AckKind {
    Ack,
    Nack,
}

#[derive(Clone, Copy)]
struct AckEvent {
    msg_id: u32,
    chunk_idx: u16,
    kind: AckKind,
}

#[derive(Clone, Copy)]
struct DataHeader {
    msg_id: u32,
    chunk_idx: u16,
    total_chunks: u16,
    payload_len: u16,
}

struct AssemblyState {
    total_chunks: u16,
    received: usize,
    chunks: Vec<Option<Vec<u8>>>,
    last_update: Instant,
}

pub struct ReliableUdp {
    socket: UdpSocket,
    data_rx: mpsc::Receiver<(SocketAddr, Vec<u8>)>,
    ack_events: Arc<(Mutex<Vec<AckEvent>>, Condvar)>,
    next_msg_id: AtomicU32,
    chunk_payload_size: usize,
    ack_timeout: Duration,
    max_retries: u32,
}

impl ReliableUdp {
    pub fn from_socket(
        socket: UdpSocket,
        max_packet_size: usize,
        ack_timeout: Duration,
        max_retries: u32,
        assembly_timeout: Duration,
    ) -> io::Result<Self> {
        if max_packet_size <= HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_packet_size is too small",
            ));
        }
        if max_packet_size > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_packet_size must fit into u16",
            ));
        }

        let recv_socket = socket.try_clone()?;
        let send_socket = socket.try_clone()?;
        let (data_tx, data_rx) = mpsc::channel::<(SocketAddr, Vec<u8>)>();
        let ack_events = Arc::new((Mutex::new(Vec::<AckEvent>::new()), Condvar::new()));
        let ack_events_thread = Arc::clone(&ack_events);
        let recv_buf_len = max_packet_size;

        thread::spawn(move || {
            let mut assemblies = HashMap::<(SocketAddr, u32), AssemblyState>::new();
            let mut recv_buf = vec![0_u8; recv_buf_len];
            loop {
                let (n, src) = match recv_socket.recv_from(&mut recv_buf) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if n < HEADER_LEN {
                    continue;
                }

                let frame = &recv_buf[..n];
                if frame[0] != MAGIC_0 || frame[1] != MAGIC_1 || frame[2] != VERSION {
                    continue;
                }

                let msg_id = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
                let chunk_idx = u16::from_be_bytes([frame[8], frame[9]]);
                let total_chunks = u16::from_be_bytes([frame[10], frame[11]]);

                match frame[3] {
                    TYPE_ACK => {
                        notify_ack(&ack_events_thread, msg_id, chunk_idx, AckKind::Ack);
                    }
                    TYPE_NACK => {
                        notify_ack(&ack_events_thread, msg_id, chunk_idx, AckKind::Nack);
                    }
                    TYPE_DATA => {
                        let payload_len = u16::from_be_bytes([frame[12], frame[13]]) as usize;
                        if HEADER_LEN + payload_len != n || total_chunks == 0 || chunk_idx >= total_chunks
                        {
                            let nack = build_control_frame(TYPE_NACK, msg_id, chunk_idx, total_chunks);
                            let _ = send_socket.send_to(&nack, src);
                            cleanup_assemblies(&mut assemblies, assembly_timeout);
                            continue;
                        }

                        let payload = &frame[HEADER_LEN..];
                        let ack = build_control_frame(TYPE_ACK, msg_id, chunk_idx, total_chunks);
                        let _ = send_socket.send_to(&ack, src);

                        let key = (src, msg_id);
                        let state = assemblies.entry(key).or_insert_with(|| AssemblyState {
                            total_chunks,
                            received: 0,
                            chunks: vec![None; total_chunks as usize],
                            last_update: Instant::now(),
                        });

                        if state.total_chunks != total_chunks {
                            let nack = build_control_frame(TYPE_NACK, msg_id, chunk_idx, total_chunks);
                            let _ = send_socket.send_to(&nack, src);
                            assemblies.remove(&key);
                            cleanup_assemblies(&mut assemblies, assembly_timeout);
                            continue;
                        }

                        state.last_update = Instant::now();
                        let idx = chunk_idx as usize;
                        if state.chunks[idx].is_none() {
                            state.chunks[idx] = Some(payload.to_vec());
                            state.received += 1;
                        }

                        if state.received == state.total_chunks as usize {
                            let mut assembled = Vec::new();
                            let mut complete = true;
                            for chunk in &state.chunks {
                                if let Some(bytes) = chunk {
                                    assembled.extend_from_slice(bytes);
                                } else {
                                    complete = false;
                                    break;
                                }
                            }
                            assemblies.remove(&key);
                            if complete {
                                let _ = data_tx.send((src, assembled));
                            }
                        }
                    }
                    _ => {}
                }

                cleanup_assemblies(&mut assemblies, assembly_timeout);
            }
        });

        Ok(Self {
            socket,
            data_rx,
            ack_events,
            next_msg_id: AtomicU32::new(1),
            chunk_payload_size: max_packet_size - HEADER_LEN,
            ack_timeout,
            max_retries,
        })
    }

    pub fn try_recv(&self) -> io::Result<Option<(SocketAddr, Vec<u8>)>> {
        match self.data_rx.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "reliable udp data channel disconnected",
            )),
        }
    }

    pub fn send_message(&self, peer: SocketAddr, data: &[u8]) -> io::Result<()> {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        let total_chunks_usize = data.len().div_ceil(self.chunk_payload_size);
        if total_chunks_usize == 0 {
            return Ok(());
        }
        if total_chunks_usize > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message is too large",
            ));
        }

        let total_chunks = total_chunks_usize as u16;
        for chunk_idx in 0..total_chunks {
            let start = chunk_idx as usize * self.chunk_payload_size;
            let end = (start + self.chunk_payload_size).min(data.len());
            let payload = &data[start..end];
            let frame = build_data_frame(DataHeader {
                msg_id,
                chunk_idx,
                total_chunks,
                payload_len: payload.len() as u16,
            }, payload);
            self.send_chunk_with_retry(peer, msg_id, chunk_idx, &frame)?;
        }
        Ok(())
    }

    fn send_chunk_with_retry(
        &self,
        peer: SocketAddr,
        msg_id: u32,
        chunk_idx: u16,
        frame: &[u8],
    ) -> io::Result<()> {
        for attempt in 0..=self.max_retries {
            self.socket.send_to(frame, peer)?;
            match self.wait_ack_event(msg_id, chunk_idx, self.ack_timeout)? {
                Some(AckKind::Ack) => return Ok(()),
                Some(AckKind::Nack) => {}
                None => {}
            }
            if attempt == self.max_retries {
                break;
            }
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("ack timeout: msg_id={msg_id}, chunk_idx={chunk_idx}"),
        ))
    }

    fn wait_ack_event(
        &self,
        msg_id: u32,
        chunk_idx: u16,
        timeout: Duration,
    ) -> io::Result<Option<AckKind>> {
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = &*self.ack_events;
        let mut events = lock
            .lock()
            .map_err(|_| io::Error::other("ack mutex poisoned"))?;

        loop {
            if let Some(pos) = events
                .iter()
                .position(|e| e.msg_id == msg_id && e.chunk_idx == chunk_idx)
            {
                let event = events.swap_remove(pos);
                return Ok(Some(event.kind));
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let wait_for = deadline.saturating_duration_since(now);
            let (guard, _) = cvar
                .wait_timeout(events, wait_for)
                .map_err(|_| io::Error::other("ack condvar poisoned"))?;
            events = guard;
        }
    }
}

fn build_data_frame(header: DataHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(MAGIC_0);
    out.push(MAGIC_1);
    out.push(VERSION);
    out.push(TYPE_DATA);
    out.extend_from_slice(&header.msg_id.to_be_bytes());
    out.extend_from_slice(&header.chunk_idx.to_be_bytes());
    out.extend_from_slice(&header.total_chunks.to_be_bytes());
    out.extend_from_slice(&header.payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn build_control_frame(kind: u8, msg_id: u32, chunk_idx: u16, total_chunks: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.push(MAGIC_0);
    out.push(MAGIC_1);
    out.push(VERSION);
    out.push(kind);
    out.extend_from_slice(&msg_id.to_be_bytes());
    out.extend_from_slice(&chunk_idx.to_be_bytes());
    out.extend_from_slice(&total_chunks.to_be_bytes());
    out.extend_from_slice(&0_u16.to_be_bytes());
    out
}

fn notify_ack(events: &Arc<(Mutex<Vec<AckEvent>>, Condvar)>, msg_id: u32, chunk_idx: u16, kind: AckKind) {
    let (lock, cvar) = &**events;
    if let Ok(mut guard) = lock.lock() {
        guard.push(AckEvent {
            msg_id,
            chunk_idx,
            kind,
        });
        cvar.notify_all();
    }
}

fn cleanup_assemblies(
    assemblies: &mut HashMap<(SocketAddr, u32), AssemblyState>,
    assembly_timeout: Duration,
) {
    let now = Instant::now();
    assemblies.retain(|_, state| now.duration_since(state.last_update) < assembly_timeout);
}
