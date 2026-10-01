use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::OwnedFd;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_UMEM_FRAME_COUNT: u32 = 4096;

const DMA_BUF_IOCTL_SYNC: libc::c_ulong = 0x4008_6200;
const DMA_BUF_SYNC_READ: u64 = 1 << 0;
const DMA_BUF_SYNC_WRITE: u64 = 2;
const DMA_BUF_SYNC_START: u64 = 0 << 2;
const DMA_BUF_SYNC_END: u64 = 1 << 2;

#[repr(C)]
struct DmaBufSync {
    flags: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UmemBackingKind {
    HostMmap,
    ImportedDmaBuf,
}

enum Backing {
    HostMmap,
    DmaBuf(File),
}

/// Experimental mapped pool; it is not automatically registered as an AF_XDP UMEM.
pub struct UmemBufferPool {
    base_ptr: NonNull<u8>,
    capacity_bytes: usize,
    frame_count: u32,
    backing: Backing,
    allocated_frames: Vec<AtomicBool>,
}

// SAFETY: mappings remain valid for the pool lifetime; allocation state grants
// each frame to at most one lease, and leases can move between threads.
unsafe impl Send for UmemBufferPool {}
// SAFETY: shared pool access only allocates distinct frames or performs DMA-BUF sync.
unsafe impl Sync for UmemBufferPool {}

impl UmemBufferPool {
    pub fn new_host_mmap(frame_count: u32) -> io::Result<Arc<Self>> {
        let (capacity_bytes, allocated_frames) = Self::pool_layout(frame_count)?;
        // SAFETY: anonymous mmap is called with a null hint and a checked nonzero length.
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                capacity_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let base_ptr = NonNull::new(mapped.cast()).ok_or_else(|| {
            // SAFETY: a successful mmap must be unmapped with the same length.
            unsafe { libc::munmap(mapped, capacity_bytes) };
            io::Error::other("mmap returned a null address")
        })?;

        Ok(Arc::new(Self {
            base_ptr,
            capacity_bytes,
            frame_count,
            backing: Backing::HostMmap,
            allocated_frames,
        }))
    }

    /// Maps a DMA-BUF FD exported by another component. The caller must ensure
    /// the FD refers to a DMA-BUF at least `frame_count * PAGE_SIZE` bytes long.
    /// This does not allocate DMA-BUF storage or register the mapping with AF_XDP.
    pub fn import_dmabuf(fd: OwnedFd, frame_count: u32) -> io::Result<Arc<Self>> {
        let (capacity_bytes, allocated_frames) = Self::pool_layout(frame_count)?;
        let file = File::from(fd);
        // SAFETY: the caller guarantees that this DMA-BUF is large enough for the mapping.
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                capacity_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                std::os::fd::AsRawFd::as_raw_fd(&file),
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let base_ptr = NonNull::new(mapped.cast()).ok_or_else(|| {
            // SAFETY: a successful mmap must be unmapped with the same length.
            unsafe { libc::munmap(mapped, capacity_bytes) };
            io::Error::other("DMA-BUF mmap returned a null address")
        })?;

        Ok(Arc::new(Self {
            base_ptr,
            capacity_bytes,
            frame_count,
            backing: Backing::DmaBuf(file),
            allocated_frames,
        }))
    }

    fn pool_layout(frame_count: u32) -> io::Result<(usize, Vec<AtomicBool>)> {
        let capacity_bytes = (frame_count as usize)
            .checked_mul(PAGE_SIZE)
            .filter(|capacity| frame_count > 0 && *capacity > 0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid frame count"))?;
        let allocated_frames = (0..frame_count).map(|_| AtomicBool::new(false)).collect();
        Ok((capacity_bytes, allocated_frames))
    }

    pub fn allocate_frame(self: &Arc<Self>) -> Option<UmemFrameLease> {
        self.allocated_frames
            .iter()
            .enumerate()
            .find_map(|(frame_index, allocated)| {
                allocated
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .ok()
                    .map(|_| UmemFrameLease {
                        pool: Arc::clone(self),
                        frame_index: frame_index as u32,
                    })
            })
    }

    pub fn sync_dmabuf(&self, start: bool, write: bool) -> io::Result<()> {
        let Backing::DmaBuf(file) = &self.backing else {
            return Ok(());
        };
        let flags = if start {
            DMA_BUF_SYNC_START
        } else {
            DMA_BUF_SYNC_END
        } | if write {
            DMA_BUF_SYNC_WRITE
        } else {
            DMA_BUF_SYNC_READ
        };
        let sync = DmaBufSync { flags };
        // SAFETY: the ioctl uses the Linux DMA_BUF_IOCTL_SYNC ABI and a live DMA-BUF FD.
        let result = unsafe {
            libc::ioctl(
                std::os::fd::AsRawFd::as_raw_fd(file),
                DMA_BUF_IOCTL_SYNC,
                &sync as *const DmaBufSync,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn backing_kind(&self) -> UmemBackingKind {
        match self.backing {
            Backing::HostMmap => UmemBackingKind::HostMmap,
            Backing::DmaBuf(_) => UmemBackingKind::ImportedDmaBuf,
        }
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }
}

impl Drop for UmemBufferPool {
    fn drop(&mut self) {
        // SAFETY: this pool exclusively owns the mapping and retains its original length.
        unsafe {
            libc::munmap(self.base_ptr.as_ptr().cast(), self.capacity_bytes);
        }
    }
}

pub struct UmemFrameLease {
    pool: Arc<UmemBufferPool>,
    frame_index: u32,
}

impl UmemFrameLease {
    pub fn frame_index(&self) -> u32 {
        self.frame_index
    }

    pub fn offset(&self) -> usize {
        self.frame_index as usize * PAGE_SIZE
    }

    pub fn len(&self) -> usize {
        PAGE_SIZE
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn bytes(&self) -> &[MaybeUninit<u8>] {
        let start = self.offset();
        // SAFETY: the lease exclusively owns this in-bounds frame; MaybeUninit
        // permits reading the mapped storage without assuming it was initialized.
        unsafe {
            std::slice::from_raw_parts(self.pool.base_ptr.as_ptr().add(start).cast(), self.len())
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let start = self.offset();
        // SAFETY: the lease exclusively owns this in-bounds frame and yields a
        // mutable slice only through a mutable borrow of the lease.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.pool.base_ptr.as_ptr().add(start).cast(),
                self.len(),
            )
        }
    }
}

impl Drop for UmemFrameLease {
    fn drop(&mut self) {
        self.pool.allocated_frames[self.frame_index as usize].store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_distinct_frames_and_releases_them_on_drop() {
        let pool = UmemBufferPool::new_host_mmap(2).unwrap();
        let first = pool.allocate_frame().unwrap();
        let second = pool.allocate_frame().unwrap();
        assert_ne!(first.frame_index(), second.frame_index());
        assert!(pool.allocate_frame().is_none());

        let released_index = first.frame_index();
        drop(first);
        let reused = pool.allocate_frame().unwrap();
        assert_eq!(reused.frame_index(), released_index);
        assert_eq!(reused.len(), PAGE_SIZE);
    }

    #[test]
    fn rejects_zero_frames() {
        match UmemBufferPool::new_host_mmap(0) {
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidInput),
            Ok(_) => panic!("zero frames must be rejected"),
        }
    }
}
