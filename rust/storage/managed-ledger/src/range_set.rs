use crate::position::ManagedLedgerPosition;
use serde::Deserialize;
use std::collections::BTreeSet;

/// A sorted, non-overlapping set of deleted-position ranges (both endpoints
/// inclusive).
///
/// Consecutive acknowledgements are coalesced into a single range, so memory
/// usage is O(number of ranges) instead of O(number of acknowledged
/// positions). A Shared-subscription cursor acknowledging millions of messages
/// in near-sequential order keeps only a handful of ranges in memory.
///
/// The representation mirrors Apache Pulsar's `individualDeletedMessages`
/// (a `LongPairRangeSet`): the ack frontier (`mark_delete`) can only express a
/// contiguous prefix, so out-of-order acknowledgements past the frontier are
/// tracked here as compact ranges instead of one entry per position.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RangeSet<K> {
    /// (start, end) pairs; ranges are kept disjoint and non-adjacent
    /// (adjacent ranges are merged on insert).
    ranges: BTreeSet<(K, K)>,
}

impl<'de, K: Ord + Deserialize<'de>> serde::Deserialize<'de> for RangeSet<K> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            ranges: BTreeSet::deserialize(deserializer)?,
        })
    }
}

impl<K> Default for RangeSet<K> {
    fn default() -> Self {
        Self {
            ranges: BTreeSet::new(),
        }
    }
}

/// Successor operation on a position key, used for adjacency checks during
/// range merging. Returns `None` on overflow (e.g. `u64::MAX`), which simply
/// disables adjacency merging at that boundary.
pub trait Succ: Ord + Sized {
    fn succ(&self) -> Option<Self>;
}

impl Succ for u64 {
    fn succ(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Succ for ManagedLedgerPosition {
    fn succ(&self) -> Option<Self> {
        self.entry_id.checked_add(1).map(|entry_id| ManagedLedgerPosition {
            ledger_id: self.ledger_id,
            entry_id,
            partition: self.partition,
        })
    }
}

impl<K: Ord + Clone + Succ> RangeSet<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
    }

    /// Number of stored ranges (the memory-relevant metric).
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, K)> {
        self.ranges.iter()
    }

    /// Returns `true` if `pos` falls inside any stored range.
    ///
    /// Two lookups are needed because a range starting exactly at `pos` sorts
    /// after the `(pos, pos)` probe when its end is greater than `pos`.
    pub fn contains(&self, pos: &K) -> bool {
        // 1) The largest range whose start is <= pos.
        if let Some((start, end)) = self.ranges.range(..=(pos.clone(), pos.clone())).next_back() {
            if start <= pos && pos <= end {
                return true;
            }
        }
        // 2) A range starting exactly at pos.
        if let Some((start, _)) = self.ranges.range((pos.clone(), pos.clone())..).next() {
            if start == pos {
                return true;
            }
        }
        false
    }

    /// Inserts the inclusive range `[start, end]`, merging every stored range
    /// that intersects or is adjacent to it.
    pub fn insert_range(&mut self, start: K, end: K) {
        if start > end {
            return;
        }
        let mut start = start;
        let mut end = end;

        // Merge ranges starting at or after the new start that intersect or
        // are adjacent to the growing range. Adjacency is `s <= end + 1`.
        loop {
            let limit = end.succ().unwrap_or_else(|| end.clone());
            let merge = self
                .ranges
                .range((start.clone(), start.clone())..)
                .next()
                .cloned()
                .filter(|(s, _)| *s <= limit);
            let Some((s, e)) = merge else {
                break;
            };
            self.ranges.remove(&(s.clone(), e.clone()));
            if s < start {
                start = s;
            }
            if end < e {
                end = e;
            }
        }

        // Merge the left neighbour when it is adjacent to the new start.
        if let Some((s, e)) = self
            .ranges
            .range(..=(start.clone(), start.clone()))
            .next_back()
            .cloned()
        {
            if e.succ() == Some(start.clone()) {
                self.ranges.remove(&(s.clone(), e.clone()));
                start = s;
            }
        }

        self.ranges.insert((start, end));
    }

    /// Inserts a single position (a one-point range).
    pub fn insert(&mut self, pos: K) {
        if self.contains(&pos) {
            return;
        }
        self.insert_range(pos.clone(), pos);
    }

    /// If `pos` falls inside a stored range, removes that range and returns
    /// its end. Used by mark-delete advancement: the whole contiguous deleted
    /// segment can be skipped in one step instead of one position at a time.
    pub fn take_covering(&mut self, pos: &K) -> Option<K> {
        // 1) The largest range whose start is <= pos.
        if let Some((start, end)) = self
            .ranges
            .range(..=(pos.clone(), pos.clone()))
            .next_back()
            .cloned()
        {
            if start <= *pos {
                self.ranges.remove(&(start.clone(), end.clone()));
                return Some(end);
            }
        }
        // 2) A range starting exactly at pos.
        if let Some((start, end)) = self
            .ranges
            .range((pos.clone(), pos.clone())..)
            .next()
            .cloned()
        {
            if start == *pos {
                self.ranges.remove(&(start.clone(), end.clone()));
                return Some(end);
            }
        }
        None
    }
}
