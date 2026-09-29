use std::io::ErrorKind;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const STATE_IDLE: u8 = 0;
const STATE_SPECULATIVE_FLIGHT: u8 = 1;
const STATE_SENT: u8 = 2;
const STATE_VALIDATED_MATCH: u8 = 3;
const STATE_MISPREDICTION_INVALIDATED: u8 = 4;
const STATE_SEND_FAILED: u8 = 5;

const EXPERT_COUNT: u8 = 8;
const TOKEN_COUNT: usize = 10_000;
const RECEIVE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

#[repr(C, align(64))]
pub struct SpeculativeTokenContext {
    pub token_id: u64,
    pub payload_bytes: [u8; 64],
    pub predicted_expert_id: u8,
    pub actual_expert_id: AtomicU8,
    pub state_flag: AtomicU8,
}

fn main() {
    println!("===============================================================");
    println!("   SPECULATIVE PRE-ROUTING SIMULATION");
    println!("===============================================================");

    let receiver_socket =
        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind speculative receiver socket");
    if let Err(error) =
        socket2::SockRef::from(&receiver_socket).set_recv_buffer_size(RECEIVE_BUFFER_BYTES)
    {
        eprintln!("Unable to set speculative receiver buffer size: {error}");
    }
    receiver_socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("Failed to configure receiver timeout");
    let receiver_address = receiver_socket
        .local_addr()
        .expect("Failed to get receiver address");

    let sender_socket =
        UdpSocket::bind("127.0.0.1:0").expect("Failed to bind speculative sender socket");
    sender_socket
        .connect(receiver_address)
        .expect("Failed to connect speculative sender socket");

    let pool: Arc<Vec<SpeculativeTokenContext>> = Arc::new(
        (0..TOKEN_COUNT)
            .map(|index| SpeculativeTokenContext {
                token_id: index as u64,
                payload_bytes: [0x77; 64],
                predicted_expert_id: (index % EXPERT_COUNT as usize) as u8,
                actual_expert_id: AtomicU8::new(if index % 10 == 0 {
                    ((index + 1) % EXPERT_COUNT as usize) as u8
                } else {
                    (index % EXPERT_COUNT as usize) as u8
                }),
                state_flag: AtomicU8::new(STATE_IDLE),
            })
            .collect(),
    );

    let receiver = thread::spawn(move || {
        let mut packet_buffer = [0u8; 64];
        let mut received = 0usize;
        while received < TOKEN_COUNT {
            match receiver_socket.recv_from(&mut packet_buffer) {
                Ok((bytes_read, _)) if bytes_read > 0 => received += 1,
                Ok(_) => {}
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        || error.kind() == ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(error) => {
                    eprintln!("Speculative receive failed: {error}");
                    break;
                }
            }
        }
        received
    });

    let start_time = Instant::now();
    let tx_pool = Arc::clone(&pool);
    let tx_handle = thread::spawn(move || {
        let mut successful_sends = 0usize;
        for token in tx_pool.iter() {
            token
                .state_flag
                .store(STATE_SPECULATIVE_FLIGHT, Ordering::Release);
            match sender_socket.send(&token.payload_bytes) {
                Ok(_) => {
                    token.state_flag.store(STATE_SENT, Ordering::Release);
                    successful_sends += 1;
                }
                Err(error) => {
                    eprintln!(
                        "Speculative send failed for token {}: {error}",
                        token.token_id
                    );
                    token.state_flag.store(STATE_SEND_FAILED, Ordering::Release);
                }
            }
        }
        successful_sends
    });

    let validation_pool = Arc::clone(&pool);
    let validation_handle = thread::spawn(move || {
        let mut matches = 0usize;
        let mut mispredictions = 0usize;

        for token in validation_pool.iter() {
            let sent = loop {
                match token.state_flag.load(Ordering::Acquire) {
                    STATE_SENT => break true,
                    STATE_SEND_FAILED => break false,
                    _ => thread::yield_now(),
                }
            };
            if !sent {
                continue;
            }

            let actual_expert = token.actual_expert_id.load(Ordering::Acquire);
            if token.predicted_expert_id == actual_expert {
                token
                    .state_flag
                    .store(STATE_VALIDATED_MATCH, Ordering::Release);
                matches += 1;
            } else {
                token
                    .state_flag
                    .store(STATE_MISPREDICTION_INVALIDATED, Ordering::Release);
                mispredictions += 1;
            }
        }
        (matches, mispredictions)
    });

    let successful_sends = tx_handle.join().expect("Transmit lane panicked");
    let (matches, mispredictions) = validation_handle.join().expect("Validation lane panicked");
    let received = receiver.join().expect("Receiver lane panicked");
    let elapsed = start_time.elapsed();

    println!("Speculative simulation complete.");
    println!("Tokens processed             : {TOKEN_COUNT}");
    println!("Successful speculative sends: {successful_sends}");
    println!("Loopback datagrams received  : {received}");
    println!("Prediction matches          : {matches}");
    println!("Mispredictions invalidated   : {mispredictions}");
    println!("Elapsed simulation time     : {elapsed:?}");
    println!("===============================================================");
}
