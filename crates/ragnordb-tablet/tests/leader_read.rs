use ragnordb_tablet::read::{LeaderReadGate, LeaderReadGateError, ReadBarrierPosition};

#[test]
fn latest_reads_require_an_applied_current_term_barrier() {
    let mut gate = LeaderReadGate::new();

    gate.on_leader_elected(4).unwrap();

    let barrier = ReadBarrierPosition { term: 4, index: 9 };

    assert!(!gate.can_serve_latest(4));

    gate.register_barrier(barrier).unwrap();

    // Registering a barrier is not enough. The exact entry must be applied.
    assert!(!gate.can_serve_latest(4));

    gate.mark_barrier_applied(barrier).unwrap();

    assert!(gate.can_serve_latest(4));
}

#[test]
fn a_new_leader_term_invalidates_the_previous_read_barrier() {
    let mut gate = LeaderReadGate::new();

    gate.on_leader_elected(4).unwrap();

    let old_barrier = ReadBarrierPosition { term: 4, index: 9 };
    gate.register_barrier(old_barrier).unwrap();
    gate.mark_barrier_applied(old_barrier).unwrap();

    assert!(gate.can_serve_latest(4));

    gate.on_leader_elected(5).unwrap();

    assert!(!gate.can_serve_latest(5));

    assert_eq!(
        gate.register_barrier(ReadBarrierPosition { term: 4, index: 10 }),
        Err(LeaderReadGateError::BarrierTermMismatch {
            leader_term: 5,
            barrier_term: 4,
        })
    );
}

#[test]
fn mismatched_barrier_apply_does_not_open_the_read_gate() {
    let mut gate = LeaderReadGate::new();

    gate.on_leader_elected(7).unwrap();

    let expected = ReadBarrierPosition { term: 7, index: 12 };
    let received = ReadBarrierPosition { term: 7, index: 13 };

    gate.register_barrier(expected).unwrap();

    assert_eq!(
        gate.mark_barrier_applied(received),
        Err(LeaderReadGateError::AppliedPositionMismatch { expected, received })
    );

    assert!(!gate.can_serve_latest(7));
}
