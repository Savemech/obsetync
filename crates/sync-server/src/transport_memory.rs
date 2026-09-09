//! Process-wide admission for transport-owned byte buffers.
//!
//! HTTP envelopes and WS data frames share this ledger.  A caller reserves
//! its conservative peak before receiving/materialising a body and retains
//! the returned owner through response handoff.  The
//! semaphore is deliberately non-waiting: transport queues must apply
//! backpressure instead of turning saturation into an unbounded waiter list.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const PROCESS_MEMORY_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportMemoryError {
    Oversized,
    Busy,
}

#[derive(Clone)]
pub(crate) struct TransportMemoryBudget {
    permits: Arc<Semaphore>,
    capacity_bytes: usize,
}

impl TransportMemoryBudget {
    pub(crate) fn process_default() -> Self {
        Self::with_capacity(PROCESS_MEMORY_BYTES)
    }

    pub(crate) fn with_capacity(capacity_bytes: usize) -> Self {
        assert!(
            capacity_bytes > 0,
            "transport memory capacity must be positive"
        );
        assert!(
            u32::try_from(capacity_bytes).is_ok(),
            "transport memory capacity exceeds semaphore range"
        );
        Self {
            permits: Arc::new(Semaphore::new(capacity_bytes)),
            capacity_bytes,
        }
    }

    pub(crate) fn try_reserve(
        &self,
        bytes: usize,
    ) -> Result<TransportMemoryReservation, TransportMemoryError> {
        let permits = u32::try_from(bytes).map_err(|_| TransportMemoryError::Oversized)?;
        if bytes > self.capacity_bytes {
            return Err(TransportMemoryError::Oversized);
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_many_owned(permits)
            .map_err(|_| TransportMemoryError::Busy)?;
        Ok(TransportMemoryReservation { _permit: permit })
    }

    pub(crate) fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub(crate) fn available_bytes(&self) -> usize {
        self.permits.available_permits()
    }
}

/// Exact owner for one transport workset. Dropping it returns the reservation
/// on success, rejection, handler error, disconnect and task cancellation.
#[derive(Debug)]
pub(crate) struct TransportMemoryReservation {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_is_bounded_and_released_by_owner_drop() {
        let budget = TransportMemoryBudget::with_capacity(10);
        let first = budget.try_reserve(7).unwrap();
        assert_eq!(budget.available_bytes(), 3);
        assert_eq!(
            budget.try_reserve(4).unwrap_err(),
            TransportMemoryError::Busy
        );
        drop(first);
        assert_eq!(budget.available_bytes(), 10);
    }

    #[test]
    fn one_budget_is_shared_by_every_clone() {
        let budget = TransportMemoryBudget::with_capacity(10);
        let peer = budget.clone();
        let owner = budget.try_reserve(10).unwrap();
        assert_eq!(peer.try_reserve(1).unwrap_err(), TransportMemoryError::Busy);
        drop(owner);
        assert_eq!(peer.available_bytes(), 10);
    }

    #[test]
    fn impossible_request_is_distinct_from_temporary_pressure() {
        let budget = TransportMemoryBudget::with_capacity(10);
        assert_eq!(
            budget.try_reserve(11).unwrap_err(),
            TransportMemoryError::Oversized
        );
        assert_eq!(budget.available_bytes(), 10);
    }
}
