use ragnordb_common::{
    Error,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::recovery::{RecoveryAllocatorFloors, RecoveryHighWaterMarks};

/// Verifies that every recovered maximum is converted into a strictly greater
/// next-allocation value.
///
/// Realistic bug caught:
///
/// Restoring an allocator directly to its durable maximum would cause the first
/// post-recovery allocation to reuse an existing identity or timestamp.
#[test]
fn allocator_floors_are_strictly_greater_than_recovered_maxima() {
    let marks = RecoveryHighWaterMarks {
        max_transaction_id: TxnId(41),
        max_timestamp: Timestamp(90),
        max_table_id: TableId(42),
        max_snapshot_id: 8,
    };

    assert_eq!(
        marks
            .checked_allocator_floors()
            .expect("finite high-water marks must produce allocator floors"),
        RecoveryAllocatorFloors {
            next_transaction_id: TxnId(42),
            next_timestamp: Timestamp(91),
            next_table_id: TableId(43),
            next_snapshot_id: 9,
        }
    );
}

/// Verifies that exhaustion in any durable allocator namespace stops recovery.
///
/// Realistic bug caught:
///
/// An unchecked `maximum + 1` could wrap to zero and reuse reserved or already
/// durable identities after startup.
#[test]
fn exhausted_recovered_namespace_fails_before_allocator_publication() {
    let exhausted_marks = [
        RecoveryHighWaterMarks {
            max_transaction_id: TxnId(u64::MAX),
            ..RecoveryHighWaterMarks::default()
        },
        RecoveryHighWaterMarks {
            max_timestamp: Timestamp(u64::MAX),
            ..RecoveryHighWaterMarks::default()
        },
        RecoveryHighWaterMarks {
            max_table_id: TableId(u64::MAX),
            ..RecoveryHighWaterMarks::default()
        },
        RecoveryHighWaterMarks {
            max_snapshot_id: u64::MAX,
            ..RecoveryHighWaterMarks::default()
        },
    ];

    for marks in exhausted_marks {
        let error = marks.checked_allocator_floors().unwrap_err();

        assert!(matches!(
            error,
            Error::RecoveryFailed { reason }
                if reason.contains("allocator")
                    && reason.contains("exhausted")
        ));
    }
}
