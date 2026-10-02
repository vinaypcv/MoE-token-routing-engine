use moe_holistic_engine::engine::clock::measurement_now_ns;
use std::env;
use std::ffi::CString;
use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::thread;
use std::time::{Duration, Instant};

const ETH_HEADER_LEN: usize = 14;
const IPV4_HEADER_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const TOKEN_HEADER_LEN: usize = 14;
const TIMESTAMP_FEATURE_LEN: usize = 8;
const BATCH_SIZE: usize = 64;
const ZIPF_LUT_SIZE: usize = 65_536;
const ETH_P_ALL: u16 = 0x0003;

struct FrameConfig {
    source_mac: [u8; 6],
    destination_mac: [u8; 6],
    source_ip: [u8; 4],
    destination_ip: [u8; 4],
    source_port: u16,
    destination_port: u16,
    feature_bytes: usize,
}

struct ZipfSampler {
    lut: Vec<u8>,
    rng_state: u64,
}

impl ZipfSampler {
    fn new(expert_count: u8, skew: f64, seed: u64) -> Result<Self, String> {
        if expert_count == 0 || !skew.is_finite() || skew < 0.0 {
            return Err(
                "experts must be positive and Zipf skew must be finite and nonnegative".into(),
            );
        }
        let mut cumulative = Vec::with_capacity(expert_count as usize);
        let mut total = 0.0;
        for rank in 1..=expert_count {
            total += f64::from(rank).powf(-skew);
            cumulative.push(total);
        }
        let mut lut = Vec::with_capacity(ZIPF_LUT_SIZE);
        let mut rank = 0;
        for slot in 0..ZIPF_LUT_SIZE {
            let point = (slot as f64 + 0.5) * total / ZIPF_LUT_SIZE as f64;
            while rank + 1 < cumulative.len() && cumulative[rank] < point {
                rank += 1;
            }
            lut.push(rank as u8);
        }
        Ok(Self {
            lut,
            rng_state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        })
    }

    fn next(&mut self) -> u8 {
        let mut value = self.rng_state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.rng_state = value;
        self.lut[(value.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 48) as usize]
    }
}

fn parse_mac(value: &str) -> Result<[u8; 6], String> {
    let octets = value
        .split(':')
        .map(|part| u8::from_str_radix(part, 16).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    octets
        .try_into()
        .map_err(|_| "MAC address must contain six colon-separated octets".to_owned())
}

fn interface_mac(interface: &str) -> io::Result<[u8; 6]> {
    let address = fs::read_to_string(format!("/sys/class/net/{interface}/address"))?;
    parse_mac(address.trim()).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn parse<T: std::str::FromStr>(value: &str, label: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {label}: {error}"))
}

fn checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    let (pairs, _) = header.as_chunks::<2>();
    for pair in pairs {
        sum += u32::from(u16::from_be_bytes([pair[0], pair[1]]));
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn build_frame(
    config: &FrameConfig,
    token_id: u64,
    expert_id: u8,
    sequence_id: u32,
    timestamp_ns: u64,
) -> Vec<u8> {
    let udp_length = UDP_HEADER_LEN + TOKEN_HEADER_LEN + config.feature_bytes;
    let ip_length = IPV4_HEADER_LEN + udp_length;
    let mut frame = vec![0u8; ETH_HEADER_LEN + ip_length];
    frame[..6].copy_from_slice(&config.destination_mac);
    frame[6..12].copy_from_slice(&config.source_mac);
    frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes());

    let ip = ETH_HEADER_LEN;
    frame[ip] = 0x45;
    frame[ip + 2..ip + 4].copy_from_slice(&(ip_length as u16).to_be_bytes());
    frame[ip + 4..ip + 6].copy_from_slice(&(sequence_id as u16).to_be_bytes());
    frame[ip + 8] = 64;
    frame[ip + 9] = 17;
    frame[ip + 12..ip + 16].copy_from_slice(&config.source_ip);
    frame[ip + 16..ip + 20].copy_from_slice(&config.destination_ip);
    let ip_checksum = checksum(&frame[ip..ip + IPV4_HEADER_LEN]);
    frame[ip + 10..ip + 12].copy_from_slice(&ip_checksum.to_be_bytes());

    let udp = ip + IPV4_HEADER_LEN;
    frame[udp..udp + 2].copy_from_slice(&config.source_port.to_be_bytes());
    frame[udp + 2..udp + 4].copy_from_slice(&config.destination_port.to_be_bytes());
    frame[udp + 4..udp + 6].copy_from_slice(&(udp_length as u16).to_be_bytes());

    let token = udp + UDP_HEADER_LEN;
    frame[token] = 0x77;
    frame[token + 1..token + 9].copy_from_slice(&token_id.to_be_bytes());
    frame[token + 9] = expert_id;
    frame[token + 10..token + 14].copy_from_slice(&sequence_id.to_be_bytes());
    frame[token + TOKEN_HEADER_LEN..token + TOKEN_HEADER_LEN + 8]
        .copy_from_slice(&timestamp_ns.to_be_bytes());
    frame
}

fn send_batch(socket_fd: RawFd, messages: &mut [libc::mmsghdr]) -> io::Result<usize> {
    loop {
        // SAFETY: each message references a live frame buffer and sockaddr_ll for this call.
        let sent = unsafe {
            libc::sendmmsg(
                socket_fd,
                messages.as_mut_ptr(),
                messages.len() as libc::c_uint,
                0,
            )
        };
        if sent >= 0 {
            return Ok(sent as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 10 {
        return Err("usage: raw_packet_profiler <iface> <dest-mac> <src-ip> <dst-ip> <src-port> <dst-port> <seconds> <pps> <zipf-skew> <seed>".into());
    }
    let interface = &arguments[0];
    let destination_mac = parse_mac(&arguments[1])?;
    let source_ip = parse::<Ipv4Addr>(&arguments[2], "source IPv4")?.octets();
    let destination_ip = parse::<Ipv4Addr>(&arguments[3], "destination IPv4")?.octets();
    let source_port = parse::<u16>(&arguments[4], "source port")?;
    let destination_port = parse::<u16>(&arguments[5], "destination port")?;
    let duration_seconds = parse::<u64>(&arguments[6], "duration")?;
    let packets_per_second = parse::<u64>(&arguments[7], "packet rate")?;
    let skew = parse::<f64>(&arguments[8], "Zipf skew")?;
    let seed = parse::<u64>(&arguments[9], "seed")?;
    let expert_count = env::var("MOE_RAW_EXPERTS")
        .map(|value| parse::<u8>(&value, "expert count"))
        .unwrap_or(Ok(8))?;
    let feature_bytes = env::var("MOE_RAW_FEATURE_BYTES")
        .map(|value| parse::<usize>(&value, "feature bytes"))
        .unwrap_or(Ok(64))?;
    if duration_seconds == 0 || packets_per_second == 0 || expert_count == 0 || expert_count > 8 {
        return Err("duration/rate must be positive and expert count must be 1..=8".into());
    }
    if feature_bytes < TIMESTAMP_FEATURE_LEN {
        return Err("feature bytes must be at least 8 for the T0 timestamp".into());
    }

    let source_mac = interface_mac(interface)?;
    let frame_config = FrameConfig {
        source_mac,
        destination_mac,
        source_ip,
        destination_ip,
        source_port,
        destination_port,
        feature_bytes,
    };
    let interface_c = CString::new(interface.as_str())?;
    // SAFETY: if_nametoindex reads the NUL-terminated interface name.
    let interface_index = unsafe { libc::if_nametoindex(interface_c.as_ptr()) };
    if interface_index == 0 {
        return Err(io::Error::last_os_error().into());
    }

    let protocol = i32::from(ETH_P_ALL.to_be());
    // SAFETY: creating a packet socket has no pointer arguments.
    let socket_fd = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW, protocol) };
    if socket_fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let socket = SocketGuard(socket_fd);
    let mut destination: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    destination.sll_family = libc::AF_PACKET as libc::c_ushort;
    destination.sll_protocol = ETH_P_ALL.to_be();
    destination.sll_ifindex = interface_index as libc::c_int;
    destination.sll_halen = 6;
    destination.sll_addr[..6].copy_from_slice(&destination_mac);
    // SAFETY: bind consumes a valid sockaddr_ll matching this packet socket.
    if unsafe {
        libc::bind(
            socket.0,
            (&destination as *const libc::sockaddr_ll).cast(),
            std::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }

    let mut sampler = ZipfSampler::new(expert_count, skew, seed)?;
    let mut expert_sequences = [0u32; 8];
    let mut total_sent = 0u64;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(duration_seconds) {
        let mut frames = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let expert_id = sampler.next();
            expert_sequences[expert_id as usize] =
                expert_sequences[expert_id as usize].wrapping_add(1);
            let sequence_id = expert_sequences[expert_id as usize];
            frames.push(build_frame(
                &frame_config,
                total_sent + frames.len() as u64 + 1,
                expert_id,
                sequence_id,
                0,
            ));
        }

        let mut vectors = frames
            .iter_mut()
            .map(|frame| libc::iovec {
                iov_base: frame.as_mut_ptr().cast(),
                iov_len: frame.len(),
            })
            .collect::<Vec<_>>();
        let mut addresses = vec![destination; BATCH_SIZE];
        let mut messages = vectors
            .iter_mut()
            .zip(addresses.iter_mut())
            .map(|(vector, address)| libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: (address as *mut libc::sockaddr_ll).cast(),
                    msg_namelen: std::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
                    msg_iov: vector,
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            })
            .collect::<Vec<_>>();
        let mut batch_offset = 0;
        while batch_offset < BATCH_SIZE {
            let t0 = measurement_now_ns()?.to_be_bytes();
            for frame in frames.iter_mut().skip(batch_offset) {
                let timestamp_offset =
                    ETH_HEADER_LEN + IPV4_HEADER_LEN + UDP_HEADER_LEN + TOKEN_HEADER_LEN;
                frame[timestamp_offset..timestamp_offset + TIMESTAMP_FEATURE_LEN]
                    .copy_from_slice(&t0);
            }
            let sent = send_batch(socket.0, &mut messages[batch_offset..])?;
            if sent == 0 {
                thread::yield_now();
                continue;
            }
            batch_offset += sent;
            total_sent += sent as u64;
        }

        let target_elapsed = Duration::from_secs_f64(total_sent as f64 / packets_per_second as f64);
        if let Some(delay) = target_elapsed.checked_sub(started.elapsed()) {
            thread::sleep(delay);
        }
    }

    println!("interface={interface} experts={expert_count} skew={skew} seed={seed}");
    println!("raw Ethernet frames sent: {total_sent}");
    println!("elapsed_seconds: {:.6}", started.elapsed().as_secs_f64());
    println!("This is an AF_PACKET sender; run it on a peer so frames ingress the DUT NIC.");
    Ok(())
}

struct SocketGuard(RawFd);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        // SAFETY: this guard owns the packet socket descriptor.
        unsafe { libc::close(self.0) };
    }
}

#[cfg(not(target_os = "linux"))]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    Err("raw_packet_profiler requires Linux AF_PACKET".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_contains_v2_header_timestamp_and_valid_ipv4_checksum() {
        let config = FrameConfig {
            source_mac: [1, 2, 3, 4, 5, 6],
            destination_mac: [6, 5, 4, 3, 2, 1],
            source_ip: [192, 0, 2, 1],
            destination_ip: [192, 0, 2, 2],
            source_port: 9000,
            destination_port: 9001,
            feature_bytes: 16,
        };
        let frame = build_frame(&config, 42, 3, 7, 123456);
        assert_eq!(frame.len(), 14 + 20 + 8 + TOKEN_HEADER_LEN + 16);
        let token_offset = ETH_HEADER_LEN + IPV4_HEADER_LEN + UDP_HEADER_LEN;
        assert_eq!(frame[token_offset], 0x77);
        assert_eq!(frame[token_offset + 9], 3);
        assert_eq!(
            &frame[token_offset + 10..token_offset + 14],
            &7u32.to_be_bytes()
        );
        assert_eq!(
            &frame[token_offset + TOKEN_HEADER_LEN..token_offset + TOKEN_HEADER_LEN + 8],
            &123456u64.to_be_bytes()
        );
        assert_eq!(
            checksum(&frame[ETH_HEADER_LEN..ETH_HEADER_LEN + IPV4_HEADER_LEN]),
            0
        );
    }
}
