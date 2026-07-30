//! The clock-sync service thread: drives the Welch-Lynch state machine
//! against real time and the QUIC datagram transport. Per round: broadcast
//! our pulse at `next`, drain inbound pulses, close at `next + W` and apply
//! the correction to the shared [`SyncedClock`]. When a round cannot find a
//! synchronized quorum, the service drives the panic recovery loop.

use {
    crate::{
        ABSORPTION_HALF_WIDTH, ACCEPTANCE_WINDOW, PANIC_INTERVAL, PANIC_OBSERVATION_TTL,
        PULSE_PERIOD,
        clock::{LocalNs, SyncedClock},
        delay::DelayTracker,
        protocol::{self, Message},
        recovery::{PanicAction, PanicReject, PanicTracker},
        stats::RoundStats,
        welch_lynch::{Config, PulseReject, RoundOutcome, WelchLynch},
    },
    agave_votor_transport::endpoint::Datagram,
    arc_swap::ArcSwap,
    bytes::Bytes,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    log::{info, warn},
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    },
    tokio::sync::mpsc,
};

/// Upper bound on a single ingress wait so the exit flag is honored promptly.
const MAX_WAIT: Duration = Duration::from_millis(100);

/// Shared view of the current-epoch staked set, published by the peer-list
/// updater in `core` and read at every round close.
pub type SharedStakes = Arc<ArcSwap<HashMap<Pubkey, u64>>>;

/// The protocol's timing constants. Defaults are the production values;
/// tests shrink them to run many rounds quickly.
#[derive(Debug, Clone, Copy)]
pub struct ServiceTiming {
    /// T: pulse period.
    pub period: Duration,
    /// W: acceptance window; must be strictly less than `period`.
    pub window: Duration,
    /// S: absorption cluster half-width.
    pub absorption_half_width: Duration,
    /// d: how often an active recovery signal is retransmitted.
    pub panic_interval: Duration,
    /// Maximum age of a panic observation contributing to a quorum.
    pub panic_observation_ttl: Duration,
}

impl Default for ServiceTiming {
    fn default() -> Self {
        Self {
            period: PULSE_PERIOD,
            window: ACCEPTANCE_WINDOW,
            absorption_half_width: ABSORPTION_HALF_WIDTH,
            panic_interval: PANIC_INTERVAL,
            panic_observation_ttl: PANIC_OBSERVATION_TTL,
        }
    }
}

struct PanicBroadcast {
    bucket: u64,
    next_at: LocalNs,
    relay_only: bool,
}

pub struct ClockSyncService {
    thread: JoinHandle<()>,
    recovery_count: Arc<AtomicU64>,
}

impl ClockSyncService {
    pub fn new(
        identity: Pubkey,
        timing: ServiceTiming,
        clock: Arc<SyncedClock>,
        egress: mpsc::Sender<Bytes>,
        ingress: Receiver<Datagram>,
        stakes: SharedStakes,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let recovery_count = Arc::new(AtomicU64::new(0));
        let thread_recovery_count = recovery_count.clone();
        let thread = Builder::new()
            .name("solClockSync".to_string())
            .spawn(move || {
                run(
                    identity,
                    timing,
                    &clock,
                    &egress,
                    &ingress,
                    &stakes,
                    &exit,
                    &thread_recovery_count,
                )
            })
            .expect("spawn solClockSync");
        Self {
            thread,
            recovery_count,
        }
    }

    pub fn recovery_count(&self) -> u64 {
        self.recovery_count.load(Ordering::Relaxed)
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}

fn run(
    identity: Pubkey,
    timing: ServiceTiming,
    clock: &SyncedClock,
    egress: &mpsc::Sender<Bytes>,
    ingress: &Receiver<Datagram>,
    stakes: &SharedStakes,
    exit: &AtomicBool,
    recovery_count: &AtomicU64,
) {
    let period_ns = timing.period.as_nanos() as i64;
    let config = Config {
        period_ns,
        window_ns: timing.window.as_nanos() as i64,
        absorption_half_width_ns: timing.absorption_half_width.as_nanos() as i64,
    };
    assert!(
        !timing.panic_interval.is_zero() && timing.panic_observation_ttl >= timing.panic_interval,
        "panic timing must use a non-zero interval and a TTL at least as long as the interval"
    );

    // Bootstrap from the wall clock: the pulse at unix time k*T belongs to
    // round k, consistent across validators whose system clocks agree to
    // within ~W. Phase 2's coarse loop replaces this.
    let start_round = clock
        .local_now()
        .ns()
        .div_euclid(period_ns)
        .saturating_add(1);
    let first_next = LocalNs::from_ns(start_round.saturating_mul(period_ns));
    let mut machine = WelchLynch::new_with_offset(
        config,
        identity,
        start_round as u64,
        first_next,
        clock.offset_ns(),
    );
    info!(
        "clock-sync started: round {start_round}, T {:?}, W {:?}",
        timing.period, timing.window
    );

    let mut delays = DelayTracker::new();
    let mut stats = RoundStats::default();
    let mut pulse_sent = false;
    let mut panic_tracker = PanicTracker::new(identity);
    let mut panic_broadcast: Option<PanicBroadcast> = None;
    let mut synchronized_once = false;

    while !exit.load(Ordering::Relaxed) {
        let now = clock.local_now();
        let round_due = if pulse_sent {
            machine.round_close_at()
        } else {
            machine.pulse_due_at()
        };
        let due = panic_broadcast
            .as_ref()
            .map_or(round_due, |panic| round_due.min(panic.next_at));

        if panic_broadcast
            .as_ref()
            .is_some_and(|panic| now >= panic.next_at)
        {
            let bucket = panic_broadcast
                .as_ref()
                .expect("checked immediately above")
                .bucket;
            send_panic(bucket, egress, &mut stats);
            let stakes_snapshot = stakes.load();
            let action = if panic_broadcast
                .as_ref()
                .is_some_and(|panic| panic.relay_only)
            {
                PanicAction::None
            } else {
                panic_tracker.record_local(
                    bucket,
                    now,
                    timing.panic_observation_ttl.as_nanos() as i64,
                    &stakes_snapshot,
                )
            };
            if let Some(panic) = panic_broadcast.as_mut() {
                panic.next_at = now.saturating_add(timing.panic_interval.as_nanos() as i64);
            }
            apply_panic_action(
                action,
                &mut panic_tracker,
                &mut panic_broadcast,
                &mut machine,
                clock,
                timing,
                &stakes_snapshot,
                &mut pulse_sent,
                &mut stats,
                recovery_count,
            );
            continue;
        }

        if now >= round_due {
            if pulse_sent {
                let outcome = close_round(&mut machine, clock, stakes, &delays, &mut stats);
                let stakes_snapshot = stakes.load();
                delays.retain(|peer| stakes_snapshot.contains_key(peer));
                pulse_sent = false;
                match outcome {
                    RoundOutcome::NoQuorum { .. } if synchronized_once => {
                        if panic_broadcast
                            .as_ref()
                            .is_some_and(|panic| panic.relay_only)
                        {
                            panic_broadcast = None;
                        }
                        let action = arm_panic(
                            &mut panic_tracker,
                            &mut panic_broadcast,
                            clock,
                            timing,
                            &stakes_snapshot,
                        );
                        apply_panic_action(
                            action,
                            &mut panic_tracker,
                            &mut panic_broadcast,
                            &mut machine,
                            clock,
                            timing,
                            &stakes_snapshot,
                            &mut pulse_sent,
                            &mut stats,
                            recovery_count,
                        );
                    }
                    RoundOutcome::Midpoint { .. } | RoundOutcome::Absorption { .. } => {
                        synchronized_once = true;
                        panic_broadcast = None;
                    }
                    RoundOutcome::NoQuorum { .. } => {}
                }
            } else {
                send_pulse(&machine, clock, egress, &mut stats);
                machine.record_own_pulse();
                pulse_sent = true;
            }
            continue;
        }

        let wait = Duration::from_nanos(due.saturating_sub(now) as u64).min(MAX_WAIT);
        match ingress.recv_timeout(wait) {
            Ok(datagram) => {
                let stakes_snapshot = stakes.load();
                let action = handle_datagram(
                    datagram,
                    &mut machine,
                    clock,
                    &mut delays,
                    &mut panic_tracker,
                    timing,
                    &stakes_snapshot,
                    &mut stats,
                );
                apply_panic_action(
                    action,
                    &mut panic_tracker,
                    &mut panic_broadcast,
                    &mut machine,
                    clock,
                    timing,
                    &stakes_snapshot,
                    &mut pulse_sent,
                    &mut stats,
                    recovery_count,
                );
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                warn!("clock-sync ingress disconnected; exiting");
                break;
            }
        }
    }
}

fn handle_datagram(
    datagram: Datagram,
    machine: &mut WelchLynch,
    clock: &SyncedClock,
    delays: &mut DelayTracker,
    panic_tracker: &mut PanicTracker,
    timing: ServiceTiming,
    stakes: &HashMap<Pubkey, u64>,
    stats: &mut RoundStats,
) -> PanicAction {
    let Datagram {
        peer_pubkey,
        path_rtt,
        message,
        ..
    } = datagram;
    // Arrival is stamped at dequeue, not at the socket read; the ingress
    // channel hop lands in the delay-uncertainty term.
    let arrival = clock.local_now();
    let Some(message) = protocol::decode(&message) else {
        stats.decode_errors = stats.decode_errors.saturating_add(1);
        return PanicAction::None;
    };
    match message {
        Message::Pulse { round, lateness_ns } => {
            delays.observe(peer_pubkey, path_rtt, arrival);
            let delay_ns = delays
                .delay_ns(&peer_pubkey, arrival)
                .expect("peer was observed just above");
            match machine.on_pulse(peer_pubkey, round, arrival, delay_ns, lateness_ns) {
                Ok(()) => {}
                Err(PulseReject::StaleRound) => {
                    stats.stale_round = stats.stale_round.saturating_add(1)
                }
                Err(PulseReject::FarFutureRound) => {
                    stats.far_future_round = stats.far_future_round.saturating_add(1)
                }
                Err(PulseReject::DuplicatePeer) => {
                    stats.duplicate_peer = stats.duplicate_peer.saturating_add(1)
                }
            }
            PanicAction::None
        }
        Message::Panic { bucket } => {
            stats.panics_received = stats.panics_received.saturating_add(1);
            match panic_tracker.on_panic(
                peer_pubkey,
                bucket,
                arrival,
                timing.panic_observation_ttl.as_nanos() as i64,
                stakes,
            ) {
                Ok(action) => action,
                Err(PanicReject::UnstakedPeer) => {
                    stats.unstaked_panics = stats.unstaked_panics.saturating_add(1);
                    PanicAction::None
                }
                Err(PanicReject::StaleBucket) => {
                    stats.stale_panics = stats.stale_panics.saturating_add(1);
                    PanicAction::None
                }
            }
        }
    }
}

fn send_pulse(
    machine: &WelchLynch,
    clock: &SyncedClock,
    egress: &mpsc::Sender<Bytes>,
    stats: &mut RoundStats,
) {
    let lateness_ns = clock
        .local_now()
        .saturating_sub(machine.pulse_due_at())
        .max(0);
    stats.send_lateness_ns = lateness_ns;
    let pulse = protocol::encode_pulse(machine.round(), lateness_ns);
    if egress.try_send(pulse).is_err() {
        stats.egress_full = stats.egress_full.saturating_add(1);
    }
}

fn send_panic(bucket: u64, egress: &mpsc::Sender<Bytes>, stats: &mut RoundStats) {
    if egress.try_send(protocol::encode_panic(bucket)).is_err() {
        stats.egress_full = stats.egress_full.saturating_add(1);
    }
    stats.panics_sent = stats.panics_sent.saturating_add(1);
}

fn arm_panic(
    tracker: &mut PanicTracker,
    broadcast: &mut Option<PanicBroadcast>,
    clock: &SyncedClock,
    timing: ServiceTiming,
    stakes: &HashMap<Pubkey, u64>,
) -> PanicAction {
    let now = clock.local_now();
    if broadcast.as_ref().is_none_or(|current| current.relay_only) {
        let bucket_width_ns = timing.panic_interval.as_nanos().saturating_mul(2) as i64;
        let wall_clock_bucket = now.ns().div_euclid(bucket_width_ns) as u64;
        let bucket = tracker.next_local_bucket(wall_clock_bucket);
        *broadcast = Some(PanicBroadcast {
            bucket,
            next_at: now,
            relay_only: false,
        });
        tracker.record_local(
            bucket,
            now,
            timing.panic_observation_ttl.as_nanos() as i64,
            stakes,
        )
    } else {
        PanicAction::None
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_panic_action(
    action: PanicAction,
    tracker: &mut PanicTracker,
    broadcast: &mut Option<PanicBroadcast>,
    machine: &mut WelchLynch,
    clock: &SyncedClock,
    timing: ServiceTiming,
    stakes: &HashMap<Pubkey, u64>,
    pulse_sent: &mut bool,
    stats: &mut RoundStats,
    recovery_count: &AtomicU64,
) {
    let action = if action == PanicAction::Amplify {
        stats.panic_amplifications = stats.panic_amplifications.saturating_add(1);
        arm_panic(tracker, broadcast, clock, timing, stakes)
    } else {
        action
    };
    if action != PanicAction::Recover {
        return;
    }

    if broadcast.is_none() {
        let _ = arm_panic(tracker, broadcast, clock, timing, stakes);
    }
    let first_next = clock
        .local_now()
        .saturating_add(timing.period.as_nanos() as i64);
    machine.restart(first_next);
    clock.set_offset_ns(machine.cumulative_offset_ns());
    tracker.complete_recovery();
    if let Some(panic) = broadcast {
        panic.relay_only = true;
        panic.next_at = clock
            .local_now()
            .saturating_add(timing.panic_interval.as_nanos() as i64);
    }
    *pulse_sent = false;
    stats.recoveries = stats.recoveries.saturating_add(1);
    recovery_count.fetch_add(1, Ordering::Relaxed);
    info!("clock-sync panic quorum reached; fine loop restarted");
}

fn close_round(
    machine: &mut WelchLynch,
    clock: &SyncedClock,
    stakes: &SharedStakes,
    delays: &DelayTracker,
    stats: &mut RoundStats,
) -> RoundOutcome {
    let round = machine.round();
    let stakes = stakes.load();
    let n_peers = stakes.len();
    let outcome = machine.close_round(&stakes);
    clock.set_offset_ns(machine.cumulative_offset_ns());
    stats.report(
        round,
        &outcome,
        machine.cumulative_offset_ns(),
        clock.offset_vs_system_ns(),
        n_peers,
        delays,
        clock.local_now(),
    );
    outcome
}
