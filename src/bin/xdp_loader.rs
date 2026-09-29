use aya::programs::{Xdp, XdpFlags};
use aya::Bpf;
use std::env;
use std::error::Error;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let interface = arguments.next().unwrap_or_else(|| "lo".to_owned());
    let object_path = arguments.next().map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from("target/bpfel-unknown-none/release/libmoe_ebpf_kernel.so")
    });

    let mut bpf = Bpf::load_file(&object_path)?;
    let program: &mut Xdp = bpf
        .program_mut("xdp_moe_router")
        .ok_or("XDP program xdp_moe_router not found in object")?
        .try_into()?;
    program.load()?;
    let link_id = program.attach(&interface, XdpFlags::default())?;

    println!("XDP classifier attached to interface {interface}.");
    println!("Object: {}", object_path.display());
    println!("Press Ctrl+C to detach.");

    tokio::signal::ctrl_c().await?;
    program.detach(link_id)?;
    println!("Detached XDP classifier.");
    Ok(())
}
