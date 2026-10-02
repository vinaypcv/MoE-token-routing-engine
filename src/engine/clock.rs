use std::io;
use std::sync::OnceLock;

#[derive(Clone, Copy)]
enum ClockMode {
    Monotonic,
    Tai,
}

static CLOCK_MODE: OnceLock<Result<ClockMode, String>> = OnceLock::new();

#[cfg(target_os = "linux")]
fn clock_now_ns(clock_id: libc::clockid_t) -> io::Result<u64> {
    let mut timestamp = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the supplied timespec on success.
    if unsafe { libc::clock_gettime(clock_id, timestamp.as_mut_ptr()) } != 0 {
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

#[cfg(target_os = "linux")]
pub fn monotonic_now_ns() -> io::Result<u64> {
    clock_now_ns(libc::CLOCK_MONOTONIC)
}

#[cfg(target_os = "linux")]
pub fn tai_now_ns() -> io::Result<u64> {
    clock_now_ns(libc::CLOCK_TAI)
}

#[cfg(target_os = "linux")]
pub fn measurement_now_ns() -> io::Result<u64> {
    let mode = CLOCK_MODE.get_or_init(|| match std::env::var("MOE_T0_CLOCK").as_deref() {
        Ok("monotonic") | Err(_) => Ok(ClockMode::Monotonic),
        Ok("tai") => Ok(ClockMode::Tai),
        Ok(other) => Err(format!("unsupported MOE_T0_CLOCK mode: {other}")),
    });
    match mode {
        Ok(ClockMode::Monotonic) => monotonic_now_ns(),
        Ok(ClockMode::Tai) => tai_now_ns(),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidInput, error.clone())),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn monotonic_now_ns() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "serialized monotonic timestamps require Linux CLOCK_MONOTONIC",
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn tai_now_ns() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "CLOCK_TAI timestamps require Linux",
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn measurement_now_ns() -> io::Result<u64> {
    monotonic_now_ns()
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

    #[test]
    fn tai_clock_is_available_on_linux() {
        assert!(tai_now_ns().unwrap() > 0);
    }
}
