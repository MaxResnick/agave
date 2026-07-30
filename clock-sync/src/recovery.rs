//! Stake-weighted panic amplification for the coarse recovery loop.
//!
//! QUIC authenticates every sender. A validator records at most one active
//! panic per peer, amplifies after more than one third of current-epoch stake,
//! and recovers after more than two thirds. Observations expire so unrelated
//! network disturbances cannot accumulate into a later recovery.

use {crate::clock::LocalNs, solana_pubkey::Pubkey, std::collections::HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanicAction {
    None,
    Amplify,
    Recover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanicReject {
    UnstakedPeer,
    StaleBucket,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    received_at: LocalNs,
}

pub struct PanicTracker {
    me: Pubkey,
    active: HashMap<Pubkey, Observation>,
    last_bucket: HashMap<Pubkey, u64>,
    amplified: bool,
    recovery_emitted: bool,
}

impl PanicTracker {
    pub fn new(me: Pubkey) -> Self {
        Self {
            me,
            active: HashMap::new(),
            last_bucket: HashMap::new(),
            amplified: false,
            recovery_emitted: false,
        }
    }

    /// Select a bucket newer than our previous recovery signal even when the
    /// local wall clock moved backwards.
    pub fn next_local_bucket(&self, wall_clock_bucket: u64) -> u64 {
        self.last_bucket
            .get(&self.me)
            .map_or(wall_clock_bucket, |last| {
                wall_clock_bucket.max(last.saturating_add(1))
            })
    }

    pub fn record_local(
        &mut self,
        bucket: u64,
        now: LocalNs,
        ttl_ns: i64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> PanicAction {
        self.record(self.me, bucket, now, ttl_ns, stakes)
            .unwrap_or(PanicAction::None)
    }

    pub fn on_panic(
        &mut self,
        peer: Pubkey,
        bucket: u64,
        now: LocalNs,
        ttl_ns: i64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Result<PanicAction, PanicReject> {
        self.record(peer, bucket, now, ttl_ns, stakes)
    }

    pub fn complete_recovery(&mut self) {
        self.active.clear();
        self.amplified = false;
        self.recovery_emitted = false;
    }

    fn record(
        &mut self,
        peer: Pubkey,
        bucket: u64,
        now: LocalNs,
        ttl_ns: i64,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Result<PanicAction, PanicReject> {
        if stakes.get(&peer).copied().unwrap_or_default() == 0 {
            return Err(PanicReject::UnstakedPeer);
        }
        self.prune(now, ttl_ns);

        match self.last_bucket.get(&peer).copied() {
            Some(last) if bucket < last => return Err(PanicReject::StaleBucket),
            Some(last) if bucket == last => {
                if let Some(observation) = self.active.get_mut(&peer) {
                    observation.received_at = now;
                } else {
                    return Err(PanicReject::StaleBucket);
                }
            }
            _ => {
                self.last_bucket.insert(peer, bucket);
                self.active.insert(peer, Observation { received_at: now });
            }
        }

        Ok(self.evaluate(stakes))
    }

    fn prune(&mut self, now: LocalNs, ttl_ns: i64) {
        self.active
            .retain(|_, observation| now.saturating_sub(observation.received_at) <= ttl_ns);
    }

    fn evaluate(&mut self, stakes: &HashMap<Pubkey, u64>) -> PanicAction {
        let total_stake = stakes
            .values()
            .fold(0u64, |total, stake| total.saturating_add(*stake));
        let support_stake = self.active.keys().fold(0u64, |support, peer| {
            support.saturating_add(stakes.get(peer).copied().unwrap_or_default())
        });

        if has_supermajority(support_stake, total_stake) {
            if self.recovery_emitted {
                return PanicAction::None;
            }
            self.recovery_emitted = true;
            return PanicAction::Recover;
        }
        if has_fault_threshold(support_stake, total_stake) {
            if self.amplified {
                return PanicAction::None;
            }
            self.amplified = true;
            return PanicAction::Amplify;
        }

        self.amplified = false;
        self.recovery_emitted = false;
        PanicAction::None
    }
}

fn has_fault_threshold(stake: u64, total_stake: u64) -> bool {
    total_stake > 0 && (stake as u128).saturating_mul(3) > total_stake as u128
}

fn has_supermajority(stake: u64, total_stake: u64) -> bool {
    total_stake > 0 && (stake as u128).saturating_mul(3) > (total_stake as u128).saturating_mul(2)
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    const TTL: i64 = 800;

    fn cluster(stakes: &[u64]) -> (Vec<Pubkey>, HashMap<Pubkey, u64>, PanicTracker) {
        let peers: Vec<_> = stakes.iter().map(|_| Pubkey::new_unique()).collect();
        let stake_map = peers.iter().copied().zip(stakes.iter().copied()).collect();
        let tracker = PanicTracker::new(peers[0]);
        (peers, stake_map, tracker)
    }

    #[test]
    fn byzantine_minority_cannot_amplify() {
        let (peers, stakes, mut tracker) = cluster(&[34, 33, 33]);
        assert_eq!(
            tracker.on_panic(peers[1], 7, LocalNs::from_ns(0), TTL, &stakes),
            Ok(PanicAction::None)
        );
    }

    #[test]
    fn fault_threshold_amplifies_once() {
        let (peers, stakes, mut tracker) = cluster(&[1, 1, 1, 1]);
        assert_eq!(
            tracker.on_panic(peers[1], 7, LocalNs::from_ns(0), TTL, &stakes),
            Ok(PanicAction::None)
        );
        assert_eq!(
            tracker.on_panic(peers[2], 9, LocalNs::from_ns(1), TTL, &stakes),
            Ok(PanicAction::Amplify)
        );
        assert_eq!(
            tracker.on_panic(peers[2], 9, LocalNs::from_ns(2), TTL, &stakes),
            Ok(PanicAction::None)
        );
    }

    #[test]
    fn supermajority_recovers_across_adjacent_buckets() {
        let (peers, stakes, mut tracker) = cluster(&[1, 1, 1, 1]);
        assert_eq!(
            tracker.on_panic(peers[0], 10, LocalNs::from_ns(0), TTL, &stakes),
            Ok(PanicAction::None)
        );
        assert_eq!(
            tracker.on_panic(peers[1], 10, LocalNs::from_ns(1), TTL, &stakes),
            Ok(PanicAction::Amplify)
        );
        assert_eq!(
            tracker.on_panic(peers[2], 11, LocalNs::from_ns(2), TTL, &stakes),
            Ok(PanicAction::Recover)
        );
        assert_eq!(
            tracker.on_panic(peers[3], 11, LocalNs::from_ns(3), TTL, &stakes),
            Ok(PanicAction::None)
        );
    }

    #[test]
    fn expired_observations_do_not_accumulate() {
        let (peers, stakes, mut tracker) = cluster(&[1, 1, 1, 1]);
        assert_eq!(
            tracker.on_panic(peers[1], 1, LocalNs::from_ns(0), TTL, &stakes),
            Ok(PanicAction::None)
        );
        assert_eq!(
            tracker.on_panic(peers[2], 1, LocalNs::from_ns(TTL + 1), TTL, &stakes),
            Ok(PanicAction::None)
        );
    }

    #[test]
    fn stale_and_unstaked_messages_are_rejected() {
        let (peers, mut stakes, mut tracker) = cluster(&[1, 1, 1]);
        let unstaked = Pubkey::new_unique();
        assert_eq!(
            tracker.on_panic(unstaked, 1, LocalNs::from_ns(0), TTL, &stakes),
            Err(PanicReject::UnstakedPeer)
        );
        assert_eq!(
            tracker.on_panic(peers[1], 2, LocalNs::from_ns(0), TTL, &stakes),
            Ok(PanicAction::None)
        );
        tracker.complete_recovery();
        assert_eq!(
            tracker.on_panic(peers[1], 2, LocalNs::from_ns(1), TTL, &stakes),
            Err(PanicReject::StaleBucket)
        );
        stakes.remove(&peers[1]);
        assert_eq!(
            tracker.on_panic(peers[1], 3, LocalNs::from_ns(2), TTL, &stakes),
            Err(PanicReject::UnstakedPeer)
        );
    }

    #[test]
    fn local_bucket_remains_monotone_after_clock_rollback() {
        let (peers, stakes, mut tracker) = cluster(&[1, 1, 1]);
        assert_eq!(
            tracker.record_local(10, LocalNs::from_ns(0), TTL, &stakes),
            PanicAction::None
        );
        tracker.complete_recovery();
        assert_eq!(tracker.next_local_bucket(8), 11);
        assert_eq!(peers[0], tracker.me);
    }
}
