use std::io;

#[cfg(target_os = "linux")]
pub fn monotonic_now_ns() -> io::Result<u64> {
    let mut timestamp = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the supplied timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, timestamp.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful clock_gettime call initialized the structure.
    let timestamp = unsafe { timestamp.assume_init() };
    let seconds = u64::try_from(timestamp.tv_sec)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative monotonic seconds"))?;
    let nanoseconds = u64::try_from(timestamp.tv_nsec).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "negative monotonic nanoseconds")
    })?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanoseconds))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "monotonic timestamp overflow"))
}

#[cfg(not(target_os = "linux"))]
pub fn monotonic_now_ns() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "serialized monotonic timestamps require Linux CLOCK_MONOTONIC",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_timestamp_is_nonzero_and_nondecreasing() {
        let first = monotonic_now_ns().unwrap();
        let second = monotonic_now_ns().unwrap();
        assert!(first > 0);
        assert!(second >= first);
    }
}
