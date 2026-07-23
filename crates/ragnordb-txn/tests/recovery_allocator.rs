use ragnordb_common::{
    Error,
    ids::{Timestamp, TxnId},
};
use ragnordb_txn::{LocalTransactionManager, TransactionManager};

/// Verifies that the first transaction after recovery uses the checked next
/// identity and timestamp.
///
/// Realistic bug caught:
///
/// The live transaction manager could ignore recovered floors and reuse
/// transaction ID 1 or timestamp 1 after durable history already contains
/// larger values.
#[test]
fn recovered_transaction_floors_control_first_live_allocation() {
    let mut manager = LocalTransactionManager::from_recovered_floors(TxnId(42), Timestamp(91))
        .expect("valid recovery floors must initialize the manager");

    let transaction = manager
        .begin_transaction()
        .expect("first post-recovery transaction must begin");

    assert_eq!(transaction.id(), TxnId(42));
    assert_eq!(transaction.start_ts(), Timestamp(91));

    let commit_timestamp = manager
        .allocate_commit_timestamp(transaction.start_ts())
        .expect("post-recovery commit timestamp must allocate");

    assert_eq!(commit_timestamp, Timestamp(92));
}

/// Verifies that reserved zero cannot be installed as a next allocation.
///
/// Realistic bug caught:
///
/// Accepting zero would require subtracting one while constructing internal
/// allocator state, causing underflow or silently restoring an invalid floor.
#[test]
fn zero_recovery_floor_is_rejected() {
    let transaction_error =
        LocalTransactionManager::from_recovered_floors(TxnId(0), Timestamp(91)).unwrap_err();

    assert!(matches!(
        transaction_error,
        Error::Configuration(message)
            if message.contains("transaction ID")
                && message.contains("nonzero")
    ));

    let timestamp_error =
        LocalTransactionManager::from_recovered_floors(TxnId(42), Timestamp(0)).unwrap_err();

    assert!(matches!(
        timestamp_error,
        Error::Configuration(message)
            if message.contains("timestamp")
                && message.contains("nonzero")
    ));
}
