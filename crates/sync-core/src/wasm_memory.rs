//! Advisory allocation telemetry for one compiled WASM instance.
//!
//! The wrapper counts requested Rust allocation sizes, not allocator capacity,
//! fragmentation, stack/static data, JS objects, linear-memory residency or RSS.
//! Realloc is delegated intact: its internal old/new overlap is NOT measured.
//! All trees, sorting, serde and wasm-bindgen allocations that reach this
//! instance's global allocator share these counters; this is not per-tree data.
//! Optimized-away allocations and allocations outside GlobalAlloc are absent.
//!
//! Hooks neither enforce a limit nor allocate, log, lock, or panic. Atomic
//! counters are independently sampled (not a transactional snapshot when shared
//! by threads). Saturation/underflow sets a permanent invalid flag; diagnostics
//! must not interpret invalid counters as an exact memory/accounting proof.
//!
//! Contract references:
//! <https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html>
//! <https://doc.rust-lang.org/std/alloc/struct.System.html>
//! <https://doc.rust-lang.org/std/sync/atomic/>

#[cfg(any(target_arch = "wasm32", test))]
use core::alloc::{GlobalAlloc, Layout};
#[cfg(any(target_arch = "wasm32", test))]
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
#[cfg(any(target_arch = "wasm32", test))]
use std::alloc::System;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WasmMemorySnapshot {
    pub enabled: bool,
    pub live_requested_bytes: u64,
    pub peak_requested_bytes: u64,
    pub live_allocations: u64,
    /// Successful alloc + alloc_zeroed calls, excluding realloc calls.
    pub successful_allocations: u64,
    pub successful_reallocations: u64,
    /// Failed alloc + alloc_zeroed calls returning null.
    pub allocation_failures: u64,
    /// Failed realloc calls returning null (old allocation remains owned).
    pub reallocation_failures: u64,
    pub counters_valid: bool,
    /// Current memory 0 pages times 64 KiB, not physical residency.
    pub linear_memory_bytes: Option<u64>,
}

#[cfg(any(target_arch = "wasm32", test))]
struct Counters {
    live_requested_bytes: AtomicU64,
    peak_requested_bytes: AtomicU64,
    live_allocations: AtomicU64,
    successful_allocations: AtomicU64,
    successful_reallocations: AtomicU64,
    allocation_failures: AtomicU64,
    reallocation_failures: AtomicU64,
    invalid: AtomicBool,
}

#[cfg(any(target_arch = "wasm32", test))]
impl Counters {
    const fn new() -> Self {
        Self {
            live_requested_bytes: AtomicU64::new(0),
            peak_requested_bytes: AtomicU64::new(0),
            live_allocations: AtomicU64::new(0),
            successful_allocations: AtomicU64::new(0),
            successful_reallocations: AtomicU64::new(0),
            allocation_failures: AtomicU64::new(0),
            reallocation_failures: AtomicU64::new(0),
            invalid: AtomicBool::new(false),
        }
    }

    #[inline]
    fn add(&self, counter: &AtomicU64, amount: u64) -> u64 {
        let mut previous = counter.load(Relaxed);
        loop {
            let sum = previous.checked_add(amount);
            let next = sum.unwrap_or(u64::MAX);
            match counter.compare_exchange_weak(previous, next, Relaxed, Relaxed) {
                Ok(_) => {
                    if sum.is_none() {
                        self.invalid.store(true, Relaxed);
                    }
                    return next;
                }
                Err(current) => previous = current,
            }
        }
    }

    #[inline]
    fn subtract(&self, counter: &AtomicU64, amount: u64) {
        let mut previous = counter.load(Relaxed);
        loop {
            let difference = previous.checked_sub(amount);
            let next = difference.unwrap_or(0);
            match counter.compare_exchange_weak(previous, next, Relaxed, Relaxed) {
                Ok(_) => {
                    if difference.is_none() {
                        self.invalid.store(true, Relaxed);
                    }
                    return;
                }
                Err(current) => previous = current,
            }
        }
    }

    #[inline]
    fn add_bytes(&self, bytes: usize) {
        let live = self.add(&self.live_requested_bytes, bytes as u64);
        self.peak_requested_bytes.fetch_max(live, Relaxed);
    }

    #[inline]
    fn allocated(&self, bytes: usize) {
        self.add_bytes(bytes);
        self.add(&self.live_allocations, 1);
        self.add(&self.successful_allocations, 1);
    }

    #[inline]
    fn deallocated(&self, bytes: usize) {
        self.subtract(&self.live_requested_bytes, bytes as u64);
        self.subtract(&self.live_allocations, 1);
    }

    #[inline]
    fn reallocated(&self, previous: usize, next: usize) {
        if next >= previous {
            self.add_bytes(next - previous);
        } else {
            self.subtract(&self.live_requested_bytes, (previous - next) as u64);
        }
        // A successful realloc transfers one allocation, not an additional
        // live allocation. A null result never reaches this method.
        self.add(&self.successful_reallocations, 1);
    }

    fn snapshot(&self, enabled: bool, linear_memory_bytes: Option<u64>) -> WasmMemorySnapshot {
        WasmMemorySnapshot {
            enabled,
            live_requested_bytes: self.live_requested_bytes.load(Relaxed),
            peak_requested_bytes: self.peak_requested_bytes.load(Relaxed),
            live_allocations: self.live_allocations.load(Relaxed),
            successful_allocations: self.successful_allocations.load(Relaxed),
            successful_reallocations: self.successful_reallocations.load(Relaxed),
            allocation_failures: self.allocation_failures.load(Relaxed),
            reallocation_failures: self.reallocation_failures.load(Relaxed),
            counters_valid: !self.invalid.load(Relaxed),
            linear_memory_bytes,
        }
    }
}

#[cfg(any(target_arch = "wasm32", test))]
struct CountingAllocator<A> {
    inner: A,
    counters: Counters,
}

#[cfg(any(target_arch = "wasm32", test))]
impl<A> CountingAllocator<A> {
    const fn new(inner: A) -> Self {
        Self {
            inner,
            counters: Counters::new(),
        }
    }
}

// SAFETY: Every pointer/layout operation is delegated once to the underlying
// GlobalAlloc, preserving its return value and ownership contract. Counters
// never read allocation contents, change pointers/layouts, reject requests or
// invoke code that allocates/unwinds. Atomic fields inherit Send/Sync from A;
// no unsafe Send/Sync implementation or allocator-internal lock is needed.
#[cfg(any(target_arch = "wasm32", test))]
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    // Keep instrumentation shared instead of cloning all counter operations
    // into each allocation site under release LTO.
    #[inline(never)]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid, nonzero allocation layout.
        let pointer = unsafe { self.inner.alloc(layout) };
        if pointer.is_null() {
            self.counters.add(&self.counters.allocation_failures, 1);
        } else {
            self.counters.allocated(layout.size());
        }
        pointer
    }

    #[inline(never)]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegate zero initialization, size and alignment unchanged.
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        if pointer.is_null() {
            self.counters.add(&self.counters.allocation_failures, 1);
        } else {
            self.counters.allocated(layout.size());
        }
        pointer
    }

    #[inline(never)]
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller owns this exact underlying allocation/layout.
        unsafe { self.inner.dealloc(pointer, layout) };
        self.counters.deallocated(layout.size());
    }

    #[inline(never)]
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Delegate the valid allocation and nonzero new size intact.
        // On null, the caller still owns the old pointer and all old contents.
        let replacement = unsafe { self.inner.realloc(pointer, layout, new_size) };
        if replacement.is_null() {
            self.counters.add(&self.counters.reallocation_failures, 1);
        } else {
            self.counters.reallocated(layout.size(), new_size);
        }
        replacement
    }
}

// Never replace a native executable/test harness's global allocator. Each
// separately instantiated production WASM module owns this static and its own
// linear memory; there is no shared host/global aggregate between instances.
#[cfg(all(target_arch = "wasm32", feature = "wasm", not(test)))]
#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

/// Nonallocating raw snapshot; JS encoding belongs outside the allocator hook.
/// The renderer binding must preserve the disabled/invalid distinction and
/// handle u64 values which no longer fit JavaScript's exact integer range.
pub(crate) fn snapshot() -> WasmMemorySnapshot {
    #[cfg(target_arch = "wasm32")]
    let linear_memory_bytes = Some((core::arch::wasm32::memory_size::<0>() as u64) * 65_536);
    #[cfg(not(target_arch = "wasm32"))]
    let linear_memory_bytes = None;

    #[cfg(all(target_arch = "wasm32", feature = "wasm", not(test)))]
    {
        ALLOCATOR.counters.snapshot(true, linear_memory_bytes)
    }
    #[cfg(not(all(target_arch = "wasm32", feature = "wasm", not(test))))]
    {
        WasmMemorySnapshot {
            enabled: false,
            live_requested_bytes: 0,
            peak_requested_bytes: 0,
            live_allocations: 0,
            successful_allocations: 0,
            successful_reallocations: 0,
            allocation_failures: 0,
            reallocation_failures: 0,
            counters_valid: true,
            linear_memory_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ptr::null_mut;

    fn state<A>(allocator: &CountingAllocator<A>) -> WasmMemorySnapshot {
        allocator.counters.snapshot(true, None)
    }

    #[test]
    fn aligned_allocations_are_counted_and_released_without_global_override() {
        assert!(!snapshot().enabled);
        let allocator = CountingAllocator::new(System);
        let layout = Layout::from_size_align(4096, 4096).unwrap();
        // SAFETY: Nonzero valid layout, initialized before access, freed once
        // through the same isolated allocator with the exact original layout.
        unsafe {
            let pointer = allocator.alloc(layout);
            assert!(!pointer.is_null());
            assert_eq!(pointer as usize % layout.align(), 0);
            pointer.write_bytes(0x5a, layout.size());
            assert_eq!(*pointer.add(layout.size() - 1), 0x5a);
            let allocated = state(&allocator);
            assert_eq!(allocated.live_requested_bytes, 4096);
            assert_eq!(allocated.peak_requested_bytes, 4096);
            assert_eq!(allocated.live_allocations, 1);
            assert_eq!(allocated.successful_allocations, 1);
            allocator.dealloc(pointer, layout);
        }
        let released = state(&allocator);
        assert_eq!(released.live_requested_bytes, 0);
        assert_eq!(released.live_allocations, 0);
        assert_eq!(released.peak_requested_bytes, 4096);
        assert!(released.counters_valid);
    }

    #[test]
    fn zeroed_alloc_and_grow_shrink_realloc_preserve_data_and_exact_live_count() {
        let allocator = CountingAllocator::new(System);
        let initial = Layout::from_size_align(64, 64).unwrap();
        // SAFETY: Only use each realloc's returned pointer after success;
        // layouts follow the actual current size and unchanged alignment.
        unsafe {
            let original = allocator.alloc_zeroed(initial);
            assert!(!original.is_null());
            for index in 0..64 {
                assert_eq!(*original.add(index), 0);
            }
            original.write_bytes(0x37, 64);
            let grown = allocator.realloc(original, initial, 192);
            assert!(!grown.is_null());
            for index in 0..64 {
                assert_eq!(*grown.add(index), 0x37);
            }
            let after_grow = state(&allocator);
            assert_eq!(after_grow.live_requested_bytes, 192);
            assert_eq!(after_grow.live_allocations, 1);
            assert_eq!(after_grow.successful_allocations, 1);
            assert_eq!(after_grow.successful_reallocations, 1);
            let grown_layout = Layout::from_size_align(192, 64).unwrap();
            let shrunk = allocator.realloc(grown, grown_layout, 32);
            assert!(!shrunk.is_null());
            for index in 0..32 {
                assert_eq!(*shrunk.add(index), 0x37);
            }
            assert_eq!(state(&allocator).live_requested_bytes, 32);
            let final_layout = Layout::from_size_align(32, 64).unwrap();
            let same_size = allocator.realloc(shrunk, final_layout, 32);
            assert!(!same_size.is_null());
            assert_eq!(*same_size, 0x37);
            allocator.dealloc(same_size, final_layout);
        }
        let done = state(&allocator);
        assert_eq!(done.live_requested_bytes, 0);
        assert_eq!(done.live_allocations, 0);
        assert_eq!(done.peak_requested_bytes, 192);
        assert_eq!(done.successful_reallocations, 3);
        assert_eq!(done.allocation_failures, 0);
        assert_eq!(done.reallocation_failures, 0);
        assert!(done.counters_valid);
    }

    #[derive(Default)]
    struct FailableSystem {
        fail_alloc: AtomicBool,
        fail_zeroed: AtomicBool,
        fail_realloc: AtomicBool,
    }

    // SAFETY: Failures return null without touching any existing allocation.
    // Successful calls delegate directly to System with the caller's layout.
    unsafe impl GlobalAlloc for FailableSystem {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if self.fail_alloc.load(Relaxed) {
                null_mut()
            } else {
                unsafe { System.alloc(layout) }
            }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if self.fail_zeroed.load(Relaxed) {
                null_mut()
            } else {
                unsafe { System.alloc_zeroed(layout) }
            }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if self.fail_realloc.load(Relaxed) {
                null_mut()
            } else {
                unsafe { System.realloc(pointer, layout, new_size) }
            }
        }
    }

    #[test]
    fn failures_count_without_inventing_allocations_or_losing_realloc_owner() {
        let allocator = CountingAllocator::new(FailableSystem::default());
        let layout = Layout::from_size_align(32, 8).unwrap();
        allocator.inner.fail_alloc.store(true, Relaxed);
        allocator.inner.fail_zeroed.store(true, Relaxed);
        // SAFETY: Valid layouts; failed pointers are never dereferenced or
        // deallocated. Only the actually allocated System block is accessed.
        unsafe {
            assert!(allocator.alloc(layout).is_null());
            assert!(allocator.alloc_zeroed(layout).is_null());
            let failed = state(&allocator);
            assert_eq!(failed.allocation_failures, 2);
            assert_eq!(failed.successful_allocations, 0);
            assert_eq!(failed.live_allocations, 0);
            assert_eq!(failed.live_requested_bytes, 0);
            allocator.inner.fail_alloc.store(false, Relaxed);
            let original = allocator.alloc(layout);
            assert!(!original.is_null());
            original.write_bytes(0x62, 32);
            allocator.inner.fail_realloc.store(true, Relaxed);
            assert!(allocator.realloc(original, layout, 128).is_null());
            assert_eq!(
                *original.add(31),
                0x62,
                "failed realloc altered the old owned block"
            );
            let failed_realloc = state(&allocator);
            assert_eq!(failed_realloc.reallocation_failures, 1);
            assert_eq!(failed_realloc.successful_reallocations, 0);
            assert_eq!(failed_realloc.live_requested_bytes, 32);
            assert_eq!(failed_realloc.live_allocations, 1);
            allocator.inner.fail_realloc.store(false, Relaxed);
            let replacement = allocator.realloc(original, layout, 128);
            assert!(!replacement.is_null());
            assert_eq!(*replacement.add(31), 0x62);
            allocator.dealloc(replacement, Layout::from_size_align(128, 8).unwrap());
        }
        let done = state(&allocator);
        assert_eq!(done.live_requested_bytes, 0);
        assert_eq!(done.live_allocations, 0);
        assert_eq!(done.peak_requested_bytes, 128);
        assert_eq!(done.successful_allocations, 1);
        assert_eq!(done.successful_reallocations, 1);
        assert_eq!(done.allocation_failures, 2);
        assert_eq!(done.reallocation_failures, 1);
        assert!(done.counters_valid);
    }

    #[test]
    fn separate_allocator_instances_do_not_share_measurements() {
        let first = CountingAllocator::new(System);
        let second = CountingAllocator::new(System);
        let layout = Layout::from_size_align(13, 1).unwrap();
        // SAFETY: Independent, live, nonzero blocks are freed by their owner.
        unsafe {
            let a = first.alloc(layout);
            let b = second.alloc(layout);
            assert!(!a.is_null() && !b.is_null());
            assert_eq!(state(&first).live_requested_bytes, 13);
            assert_eq!(state(&second).live_requested_bytes, 13);
            first.dealloc(a, layout);
            assert_eq!(state(&first).live_requested_bytes, 0);
            assert_eq!(state(&second).live_requested_bytes, 13);
            second.dealloc(b, layout);
        }
        assert_eq!(state(&first).successful_allocations, 1);
        assert_eq!(state(&second).successful_allocations, 1);
    }

    #[test]
    fn counter_overflow_and_underflow_saturate_and_remain_invalid() {
        // Exercise bookkeeping directly, NEVER manufacture an allocation or
        // pass an invalid pointer/layout to GlobalAlloc to trigger underflow.
        let counters = Counters::new();
        counters.live_requested_bytes.store(u64::MAX - 2, Relaxed);
        counters.add_bytes(4);
        assert_eq!(counters.live_requested_bytes.load(Relaxed), u64::MAX);
        assert_eq!(counters.peak_requested_bytes.load(Relaxed), u64::MAX);
        assert!(!counters.snapshot(true, None).counters_valid);
        counters.successful_allocations.store(u64::MAX, Relaxed);
        counters.add(&counters.successful_allocations, 1);
        assert_eq!(counters.successful_allocations.load(Relaxed), u64::MAX);
        counters.subtract(&counters.live_allocations, 1);
        assert_eq!(counters.live_allocations.load(Relaxed), 0);
        counters.subtract(&counters.live_requested_bytes, u64::MAX);
        counters.subtract(&counters.live_requested_bytes, 1);
        assert_eq!(counters.live_requested_bytes.load(Relaxed), 0);
        counters.allocated(1);
        counters.deallocated(1);
        assert!(
            !counters.snapshot(true, None).counters_valid,
            "later valid operations hid counter poison"
        );
    }

    #[test]
    fn shared_native_test_instance_counts_concurrent_calls_without_unsafe_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CountingAllocator<System>>();
        let allocator = CountingAllocator::new(System);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let allocator = &allocator;
                scope.spawn(move || {
                    for _ in 0..128 {
                        let layout = Layout::from_size_align(32 + worker, 8).unwrap();
                        // SAFETY: Each thread owns only its returned blocks;
                        // the underlying System and atomic telemetry are Sync.
                        unsafe {
                            let original = allocator.alloc_zeroed(layout);
                            assert!(!original.is_null());
                            assert_eq!(*original, 0);
                            let grown = allocator.realloc(original, layout, 64 + worker);
                            assert!(!grown.is_null());
                            assert_eq!(*grown, 0);
                            allocator
                                .dealloc(grown, Layout::from_size_align(64 + worker, 8).unwrap());
                        }
                    }
                });
            }
        });
        let done = state(&allocator);
        assert_eq!(done.live_requested_bytes, 0);
        assert_eq!(done.live_allocations, 0);
        assert_eq!(done.successful_allocations, 512);
        assert_eq!(done.successful_reallocations, 512);
        assert_eq!(done.allocation_failures, 0);
        assert_eq!(done.reallocation_failures, 0);
        assert!(done.peak_requested_bytes >= 64);
        assert!(done.counters_valid);
    }
}
