#![no_std]

use aya_ebpf::{bindings::xdp_action, macros::xdp, programs::XdpContext};
use core::mem;
use network_types::{eth::EthHdr, ip::Ipv4Hdr, udp::UdpHdr};

const IPV4_ETHERTYPE: [u8; 2] = [0x08, 0x00];
const UDP_PROTOCOL: u8 = 17;
const MOE_MAGIC: u8 = 0x77;

#[repr(C, packed)]
pub struct TokenPacketHeader {
    pub magic_byte: u8,
    pub token_id: u64,
    pub target_expert_id: u8,
}

#[inline(always)]
fn frame_has_range(ctx: &XdpContext, offset: usize, len: usize) -> bool {
    let Some(end_offset) = offset.checked_add(len) else {
        return false;
    };
    let Some(frame_end) = ctx.data().checked_add(end_offset) else {
        return false;
    };
    frame_end <= ctx.data_end()
}

#[inline(always)]
fn read_u8(ctx: &XdpContext, offset: usize) -> Result<u8, ()> {
    if !frame_has_range(ctx, offset, 1) {
        return Err(());
    }
    Ok(unsafe { *((ctx.data() + offset) as *const u8) })
}

#[inline(always)]
fn read_be_u16(ctx: &XdpContext, offset: usize) -> Result<u16, ()> {
    if !frame_has_range(ctx, offset, 2) {
        return Err(());
    }
    let high = read_u8(ctx, offset)? as u16;
    let low = read_u8(ctx, offset + 1)? as u16;
    Ok((high << 8) | low)
}

#[xdp]
pub fn xdp_moe_router(ctx: XdpContext) -> u32 {
    match try_xdp_router(&ctx) {
        Ok(action) => action,
        Err(()) => xdp_action::XDP_PASS,
    }
}

fn try_xdp_router(ctx: &XdpContext) -> Result<u32, ()> {
    let ethernet_len = EthHdr::LEN;
    let ipv4_min_len = Ipv4Hdr::LEN;
    let udp_len = UdpHdr::LEN;

    if !frame_has_range(ctx, 0, ethernet_len) {
        return Err(());
    }

    let ether_type_offset = ethernet_len - mem::size_of::<u16>();
    if read_be_u16(ctx, ether_type_offset)? != u16::from_be_bytes(IPV4_ETHERTYPE) {
        return Ok(xdp_action::XDP_PASS);
    }

    let ipv4_offset = ethernet_len;
    if !frame_has_range(ctx, ipv4_offset, ipv4_min_len) {
        return Err(());
    }

    let version_ihl = read_u8(ctx, ipv4_offset)?;
    let version = version_ihl >> 4;
    let ihl_bytes = ((version_ihl & 0x0f) as usize) * 4;
    if version != 4 || ihl_bytes < ipv4_min_len {
        return Ok(xdp_action::XDP_PASS);
    }
    if !frame_has_range(ctx, ipv4_offset, ihl_bytes) {
        return Err(());
    }

    if read_u8(ctx, ipv4_offset + 9)? != UDP_PROTOCOL {
        return Ok(xdp_action::XDP_PASS);
    }

    let udp_offset = ipv4_offset.checked_add(ihl_bytes).ok_or(())?;
    if !frame_has_range(ctx, udp_offset, udp_len) {
        return Err(());
    }

    let token_header_offset = udp_offset.checked_add(udp_len).ok_or(())?;
    let token_header_len = mem::size_of::<TokenPacketHeader>();
    if !frame_has_range(ctx, token_header_offset, token_header_len) {
        return Err(());
    }

    if read_u8(ctx, token_header_offset)? == MOE_MAGIC {
        // Classification only: no redirect map is configured yet, so preserve normal delivery.
        return Ok(xdp_action::XDP_PASS);
    }

    Ok(xdp_action::XDP_PASS)
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
