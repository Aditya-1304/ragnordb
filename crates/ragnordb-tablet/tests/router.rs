use ragnordb_common::{Error, codec::Value, ids::TableId};
use ragnordb_storage::key::encode_primary_key;
use ragnordb_tablet::HashTabletPartitioner;

const ROUTING_DOMAIN: &[u8] = b"ragnordb/tablet-hash";
const ROUTING_VERSION: u8 = 1;

fn expected_bucket(table_id: TableId, primary_key_bytes: &[u8], bucket_count: u32) -> u32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ROUTING_DOMAIN);
    hasher.update(&[ROUTING_VERSION]);
    hasher.update(&table_id.0.to_be_bytes());
    hasher.update(&(primary_key_bytes.len() as u64).to_be_bytes());
    hasher.update(primary_key_bytes);

    let digest = hasher.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);

    (u64::from_be_bytes(prefix) % u64::from(bucket_count)) as u32
}

#[test]
fn bucket_selection_is_stable_for_the_versioned_hash_input() {
    let primary_key_bytes =
        encode_primary_key(&[Value::Int(42), Value::Text("Ada".to_string())]).unwrap();
    let table_id = TableId(17);
    let bucket_count = 7;

    let expected = expected_bucket(table_id, &primary_key_bytes, bucket_count);
    let partitioner = HashTabletPartitioner::new();

    assert_eq!(
        partitioner
            .bucket_for(table_id, &primary_key_bytes, bucket_count)
            .unwrap(),
        expected
    );
    assert_eq!(
        partitioner
            .bucket_for(table_id, &primary_key_bytes, bucket_count)
            .unwrap(),
        expected
    );
}

#[test]
fn table_id_is_part_of_the_routing_identity() {
    let primary_key_bytes = encode_primary_key(&[Value::Int(42)]).unwrap();
    let bucket_count = 251;
    let first_table = TableId(17);
    let second_table = TableId(18);
    let partitioner = HashTabletPartitioner::new();

    let first_bucket = partitioner
        .bucket_for(first_table, &primary_key_bytes, bucket_count)
        .unwrap();
    let second_bucket = partitioner
        .bucket_for(second_table, &primary_key_bytes, bucket_count)
        .unwrap();

    assert_eq!(
        first_bucket,
        expected_bucket(first_table, &primary_key_bytes, bucket_count)
    );
    assert_eq!(
        second_bucket,
        expected_bucket(second_table, &primary_key_bytes, bucket_count)
    );
    assert_ne!(first_bucket, second_bucket);
}

#[test]
fn invalid_hash_inputs_are_rejected_without_modulo_or_decode_panics() {
    let partitioner = HashTabletPartitioner::new();
    let primary_key_bytes = encode_primary_key(&[Value::Int(1)]).unwrap();

    assert!(matches!(
        partitioner.bucket_for(TableId(1), &primary_key_bytes, 0),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        partitioner.bucket_for(TableId(0), &primary_key_bytes, 1),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        partitioner.bucket_for(TableId(1), &[], 1),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        partitioner.bucket_for(TableId(1), &[0xff], 1),
        Err(Error::InvalidArgument(_))
    ));
}
