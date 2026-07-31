use std::collections::BTreeSet;

use ragnordb_common::Error;
use ragnordb_storage::wal::RagnorDbWalRecordType;
use wal::types::{
    RecordType,
    record_types::{
        BEGIN_CHECKPOINT, CHECKPOINT_CHUNK, END_CHECKPOINT, SEGMENT_MARKER, SEGMENT_SEAL, SHUTDOWN,
        USER_MIN,
    },
};

/// Freezes the durable numeric identifiers used by RagnorDB records.
///
/// These identifiers are part of the on-disk compatibility contract. Changing
/// one after a database has written WAL data would make existing records appear
/// to contain a different payload type during recovery.
#[test]
fn ragnordb_record_types_are_stable_unique_and_user_defined() {
    let expected_mappings = [
        (RagnorDbWalRecordType::SnapshotPointer, USER_MIN + 3),
        (RagnorDbWalRecordType::SingleNodeTxnCommit, USER_MIN + 5),
        (RagnorDbWalRecordType::CatalogUpdate, USER_MIN + 6),
        (RagnorDbWalRecordType::CheckpointMarker, USER_MIN + 7),
    ];

    let mut assigned_ids = BTreeSet::new();

    for (logical_type, expected_id) in expected_mappings {
        let record_type = logical_type.as_wal_record_type();
        let actual_id = record_type.as_u16();

        assert_eq!(
            actual_id, expected_id,
            "a durable RagnorDB WAL record identifier changed"
        );

        assert!(
            record_type.is_user_defined(),
            "RagnorDB records must never overlap A-WAL internal record types"
        );

        assert!(
            assigned_ids.insert(actual_id),
            "two RagnorDB WAL record kinds share record identifier {actual_id}"
        );

        assert_eq!(
            RagnorDbWalRecordType::classify(record_type).unwrap(),
            Some(logical_type)
        );
    }
}

/// Ensures valid A-WAL metadata is not passed to a RagnorDB payload decoder.
///
/// Realistic bug caught:
///
/// Recovery iterates over A-WAL's complete logical record stream, including
/// internal segment seals and shutdown witnesses. Treating those records as
/// unknown RagnorDB data would incorrectly reject a healthy WAL during startup.
#[test]
fn awal_internal_records_are_classified_outside_ragnordb_namespace() {
    let internal_record_types = [
        SEGMENT_MARKER,
        BEGIN_CHECKPOINT,
        END_CHECKPOINT,
        CHECKPOINT_CHUNK,
        SEGMENT_SEAL,
        SHUTDOWN,
    ];

    for record_type in internal_record_types {
        assert_eq!(
            RagnorDbWalRecordType::classify(record_type).unwrap(),
            None,
            "A-WAL internal record {} must not be decoded as a RagnorDB payload",
            record_type.as_u16()
        );
    }
}

/// Ensures recovery never guesses how an unrecognized user payload should be
/// decoded.
///
/// Guessing or skipping here could hide an incompatible database version or
/// cause valid bytes to be replayed using the wrong state-machine operation.
#[test]
fn unknown_user_record_type_is_rejected_as_corruption() {
    let unknown = RecordType::new(USER_MIN + 99);

    let error = RagnorDbWalRecordType::classify(unknown).unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("unknown RagnorDB user WAL record type")
    ));
}
