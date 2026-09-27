//! Comparable in-memory MVCC layouts used by the Stage 4.2 design gate.
//!
//! Every candidate stores the same encoded keys and byte values. Candidate A
//! uses one row-first ordered map, Candidate B uses three ordered family maps,
//! and Candidate C uses one family-first ordered map. A single read/write lock
//! models one coherent tablet generation and makes multi-family publication
//! visible atomically to readers.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::RwLock,
};

/// Experimental codec versions used only by the Stage 4.2 comparison.
///
/// They make the prototypes exercise versioned key and value envelopes while
/// remaining explicitly separate from any production storage contract.
pub const EXPERIMENTAL_ENCODING_VERSION: u8 = 1;
pub const BENCHMARK_WRITE_VALUE_BYTES: usize = 19;
pub const BENCHMARK_LOCK_VALUE_BYTES: usize = 27;
const KEY_FORMAT_VERSION: u8 = EXPERIMENTAL_ENCODING_VERSION;
const VALUE_FORMAT_VERSION: u8 = EXPERIMENTAL_ENCODING_VERSION;
const DEFAULT_VALUE_KIND: u8 = 0x01;
const WRITE_VALUE_KIND: u8 = 0x02;
const LOCK_VALUE_KIND: u8 = 0x03;
const LOCK_VALUE_BYTES: usize = BENCHMARK_LOCK_VALUE_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// One ordered keyspace ordered by logical row, then family, then version.
    UnifiedRowFirst,
    /// Separate default, write, and lock trees sharing one publication lock.
    SeparateFamilies,
    /// One ordered keyspace ordered by family, then logical row, then version.
    UnifiedFamilyFirst,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Namespace {
    /// MVCC row payload identified by its transaction start timestamp.
    Default = 0x10,
    /// Commit records and rollback witnesses, both identified by write time.
    Write = 0x11,
    /// Active transaction intent for a logical row.
    Lock = 0x12,
    /// Primary transaction status owned by the tablet transaction model.
    TxnPrimaryStatus = 0x20,
    /// Stable logical-command outcomes used for retry deduplication.
    RetryOutcome = 0x21,
    /// Monotonic retry/session retention floor.
    RetryFloor = 0x22,
    /// Future secondary-index entries.
    SecondaryIndex = 0x30,
    /// Future unique-value claims.
    UniqueClaim = 0x31,
}

impl Namespace {
    pub const ALL: [Self; 8] = [
        Self::Default,
        Self::Write,
        Self::Lock,
        Self::TxnPrimaryStatus,
        Self::RetryOutcome,
        Self::RetryFloor,
        Self::SecondaryIndex,
        Self::UniqueClaim,
    ];

    const fn id(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteKind {
    Put,
    Delete,
    Rollback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteRecord {
    pub start_ts: u64,
    pub commit_ts: u64,
    pub kind: WriteKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordEdit {
    pub key: Vec<u8>,
    pub namespace: Namespace,
    pub timestamp: Option<u64>,
    pub value: Option<Vec<u8>>,
}

impl RecordEdit {
    pub fn put_default(key: Vec<u8>, start_ts: u64, payload: &[u8]) -> Self {
        Self {
            key,
            namespace: Namespace::Default,
            timestamp: Some(start_ts),
            value: Some(encode_default_value(payload)),
        }
    }

    pub fn put_write(key: Vec<u8>, record: WriteRecord) -> Self {
        Self {
            key,
            namespace: Namespace::Write,
            timestamp: Some(record.commit_ts),
            value: Some(encode_write_value(record)),
        }
    }

    pub fn put_lock(key: Vec<u8>) -> Self {
        Self {
            key,
            namespace: Namespace::Lock,
            timestamp: None,
            value: Some(encode_lock_value()),
        }
    }

    pub fn delete_lock(key: Vec<u8>) -> Self {
        Self {
            key,
            namespace: Namespace::Lock,
            timestamp: None,
            value: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LayoutError {
    EmptyLogicalKey,
    InvalidTimestampShape(Namespace),
    InvalidValue(Namespace),
    DuplicateRecord,
}

#[derive(Default)]
pub struct KeyScratch {
    pub(crate) encoded: Vec<u8>,
    pub(crate) logical: Vec<u8>,
    pub(crate) prefix: Vec<u8>,
}

#[derive(Default)]
struct UnifiedRecords {
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Default)]
struct FamilyRecords {
    defaults: BTreeMap<Vec<u8>, Vec<u8>>,
    writes: BTreeMap<Vec<u8>, Vec<u8>>,
    locks: BTreeMap<Vec<u8>, Vec<u8>>,
    metadata: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Default)]
enum State {
    #[default]
    Empty,
    Unified(UnifiedRecords),
    Separate(FamilyRecords),
}

/// A benchmark-only MVCC view that gives all layouts the same publication and
/// value representation while varying only physical key ordering/tree count.
pub struct Engine {
    layout: Layout,
    state: RwLock<State>,
}

impl Engine {
    pub fn empty(layout: Layout) -> Self {
        let state = match layout {
            Layout::UnifiedRowFirst | Layout::UnifiedFamilyFirst => {
                State::Unified(UnifiedRecords::default())
            }
            Layout::SeparateFamilies => State::Separate(FamilyRecords::default()),
        };
        Self {
            layout,
            state: RwLock::new(state),
        }
    }

    /// Constructs a deterministic fixture without paying per-record
    /// publication-lock and duplicate-validation costs. The iterator is
    /// validated record-by-record before the completed generation is exposed.
    pub fn from_fixture(
        layout: Layout,
        edits: impl IntoIterator<Item = RecordEdit>,
    ) -> Result<Self, LayoutError> {
        let mut state = match layout {
            Layout::UnifiedRowFirst | Layout::UnifiedFamilyFirst => {
                State::Unified(UnifiedRecords::default())
            }
            Layout::SeparateFamilies => State::Separate(FamilyRecords::default()),
        };
        let mut scratch = KeyScratch::default();
        for edit in edits {
            validate_edit(&edit)?;
            apply_edit(layout, &mut state, &edit, &mut scratch);
        }
        Ok(Self {
            layout,
            state: RwLock::new(state),
        })
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Validates every edit before taking the write lock, then applies the full
    /// batch while readers are excluded from the tablet generation.
    pub fn publish_atomic(&self, edits: &[RecordEdit]) -> Result<(), LayoutError> {
        let mut identities = BTreeSet::new();
        let mut prepared = Vec::with_capacity(edits.len());
        let mut scratch = KeyScratch::default();

        for edit in edits {
            validate_edit(edit)?;
            physical_key_into(self.layout, edit, &mut scratch);
            let mut identity = Vec::with_capacity(scratch.encoded.len() + 1);
            identity.push(edit.namespace.id());
            identity.extend_from_slice(&scratch.encoded);
            if !identities.insert(identity) {
                return Err(LayoutError::DuplicateRecord);
            }
            prepared.push((edit.clone(), scratch.encoded.clone()));
        }

        let mut state = self
            .state
            .write()
            .expect("benchmark generation lock poisoned");
        for (edit, key) in prepared {
            apply_prepared(self.layout, &mut state, &edit, key);
        }
        Ok(())
    }

    pub fn read_at(&self, key: &[u8], read_ts: u64, scratch: &mut KeyScratch) -> Option<Vec<u8>> {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        let write = find_visible_write(self.layout, &state, key, read_ts, scratch)?;
        if write.kind == WriteKind::Delete {
            return None;
        }
        find_value(
            self.layout,
            &state,
            Namespace::Default,
            key,
            write.start_ts,
            scratch,
        )
        .map(|value| value[2..].to_vec())
    }

    pub fn read_len_at(&self, key: &[u8], read_ts: u64, scratch: &mut KeyScratch) -> Option<usize> {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        let write = find_visible_write(self.layout, &state, key, read_ts, scratch)?;
        if write.kind == WriteKind::Delete {
            return None;
        }
        find_value(
            self.layout,
            &state,
            Namespace::Default,
            key,
            write.start_ts,
            scratch,
        )
        .map(|value| value.len() - 2)
    }

    pub fn contains_lock(&self, key: &[u8], scratch: &mut KeyScratch) -> bool {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        find_value(self.layout, &state, Namespace::Lock, key, 0, scratch).is_some()
    }

    pub fn has_rollback_witness(
        &self,
        key: &[u8],
        start_ts: u64,
        scratch: &mut KeyScratch,
    ) -> bool {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        find_value(
            self.layout,
            &state,
            Namespace::Write,
            key,
            start_ts,
            scratch,
        )
        .and_then(decode_write_value)
        .is_some_and(|record| record.kind == WriteKind::Rollback)
    }

    pub fn validate_prewrite(&self, key: &[u8], start_ts: u64, scratch: &mut KeyScratch) -> bool {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        if find_value(self.layout, &state, Namespace::Lock, key, 0, scratch).is_some() {
            return false;
        }
        find_visible_write(self.layout, &state, key, u64::MAX, scratch)
            .is_none_or(|write| write.commit_ts <= start_ts)
    }

    /// Returns the first `limit` visible rows in `[start, end)` at `read_ts`.
    /// The row-first candidate seeks directly to each next row's Write range,
    /// avoiding a walk through that row's unrelated Default and Lock records.
    pub fn scan_page(
        &self,
        start: &[u8],
        end: &[u8],
        read_ts: u64,
        limit: usize,
        scratch: &mut KeyScratch,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        let mut output = Vec::with_capacity(limit);
        let mut cursor = scan_start_key(self.layout, start, read_ts, scratch);

        while output.len() < limit {
            let candidate = match &*state {
                State::Unified(unified) => {
                    next_unified_candidate(self.layout, &unified.records, &cursor, read_ts, scratch)
                }
                State::Separate(families) => {
                    next_tree_candidate(&families.writes, &cursor, read_ts, scratch)
                }
                State::Empty => None,
            };
            let Some((logical_key, write)) = candidate else {
                break;
            };
            if logical_key.as_slice() >= end {
                break;
            }
            next_row_cursor(self.layout, &logical_key, &mut cursor);
            if write.kind == WriteKind::Delete {
                continue;
            }
            if let Some(value) = find_value(
                self.layout,
                &state,
                Namespace::Default,
                &logical_key,
                write.start_ts,
                scratch,
            ) {
                output.push((logical_key, value[2..].to_vec()));
            }
        }
        output
    }

    pub fn record_count(&self) -> usize {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        match &*state {
            State::Unified(unified) => unified.records.len(),
            State::Separate(families) => {
                families.defaults.len()
                    + families.writes.len()
                    + families.locks.len()
                    + families.metadata.len()
            }
            State::Empty => 0,
        }
    }

    pub fn encoded_key_bytes(&self) -> usize {
        let state = self
            .state
            .read()
            .expect("benchmark generation lock poisoned");
        match &*state {
            State::Unified(unified) => unified.records.keys().map(Vec::len).sum(),
            State::Separate(families) => {
                families.defaults.keys().map(Vec::len).sum::<usize>()
                    + families.writes.keys().map(Vec::len).sum::<usize>()
                    + families.locks.keys().map(Vec::len).sum::<usize>()
                    + families.metadata.keys().map(Vec::len).sum::<usize>()
            }
            State::Empty => 0,
        }
    }
}

pub fn encode_default_value(payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(payload.len() + 2);
    encoded.push(VALUE_FORMAT_VERSION);
    encoded.push(DEFAULT_VALUE_KIND);
    encoded.extend_from_slice(payload);
    encoded
}

pub fn encode_write_value(record: WriteRecord) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(BENCHMARK_WRITE_VALUE_BYTES);
    encoded.push(VALUE_FORMAT_VERSION);
    encoded.push(WRITE_VALUE_KIND);
    encoded.push(match record.kind {
        WriteKind::Put => 0x01,
        WriteKind::Delete => 0x02,
        WriteKind::Rollback => 0x03,
    });
    encoded.extend_from_slice(&record.start_ts.to_be_bytes());
    encoded.extend_from_slice(&record.commit_ts.to_be_bytes());
    encoded
}

pub fn encode_lock_value() -> Vec<u8> {
    // The fixed-width fixture carries transaction ID, start timestamp, TTL,
    // and operation fields after its version and record-kind bytes.
    let mut encoded = vec![0; LOCK_VALUE_BYTES];
    encoded[0] = VALUE_FORMAT_VERSION;
    encoded[1] = LOCK_VALUE_KIND;
    encoded
}

fn decode_write_value(value: &[u8]) -> Option<WriteRecord> {
    if value.len() != 19 || value[0] != VALUE_FORMAT_VERSION || value[1] != WRITE_VALUE_KIND {
        return None;
    }
    let kind = match value[2] {
        0x01 => WriteKind::Put,
        0x02 => WriteKind::Delete,
        0x03 => WriteKind::Rollback,
        _ => return None,
    };
    Some(WriteRecord {
        kind,
        start_ts: u64::from_be_bytes(value[3..11].try_into().ok()?),
        commit_ts: u64::from_be_bytes(value[11..19].try_into().ok()?),
    })
}

fn validate_edit(edit: &RecordEdit) -> Result<(), LayoutError> {
    if edit.key.is_empty() {
        return Err(LayoutError::EmptyLogicalKey);
    }
    let timestamp_shape_is_valid = match edit.namespace {
        Namespace::Default | Namespace::Write => edit.timestamp.is_some(),
        Namespace::Lock => edit.timestamp.is_none(),
        Namespace::TxnPrimaryStatus
        | Namespace::RetryOutcome
        | Namespace::RetryFloor
        | Namespace::SecondaryIndex
        | Namespace::UniqueClaim => true,
    };
    if !timestamp_shape_is_valid {
        return Err(LayoutError::InvalidTimestampShape(edit.namespace));
    }
    if let Some(value) = &edit.value {
        let valid = match edit.namespace {
            Namespace::Default => {
                value.get(..2) == Some(&[VALUE_FORMAT_VERSION, DEFAULT_VALUE_KIND])
            }
            Namespace::Write => decode_write_value(value).is_some(),
            Namespace::Lock => {
                value.len() == LOCK_VALUE_BYTES
                    && value.get(..2) == Some(&[VALUE_FORMAT_VERSION, LOCK_VALUE_KIND])
            }
            Namespace::TxnPrimaryStatus
            | Namespace::RetryOutcome
            | Namespace::RetryFloor
            | Namespace::SecondaryIndex
            | Namespace::UniqueClaim => value.first() == Some(&VALUE_FORMAT_VERSION),
        };
        if !valid {
            return Err(LayoutError::InvalidValue(edit.namespace));
        }
    }
    Ok(())
}

fn apply_edit(layout: Layout, state: &mut State, edit: &RecordEdit, scratch: &mut KeyScratch) {
    physical_key_into(layout, edit, scratch);
    apply_prepared_key(layout, state, edit, &scratch.encoded);
}

fn apply_prepared(layout: Layout, state: &mut State, edit: &RecordEdit, physical_key: Vec<u8>) {
    apply_prepared_key(layout, state, edit, &physical_key);
}

fn apply_prepared_key(layout: Layout, state: &mut State, edit: &RecordEdit, physical_key: &[u8]) {
    match state {
        State::Unified(unified) => match &edit.value {
            Some(value) => {
                unified.records.insert(physical_key.to_vec(), value.clone());
            }
            None => {
                unified.records.remove(physical_key);
            }
        },
        State::Separate(families) => {
            let tree = match edit.namespace {
                Namespace::Default => Some(&mut families.defaults),
                Namespace::Write => Some(&mut families.writes),
                Namespace::Lock => Some(&mut families.locks),
                Namespace::TxnPrimaryStatus
                | Namespace::RetryOutcome
                | Namespace::RetryFloor
                | Namespace::SecondaryIndex
                | Namespace::UniqueClaim => Some(&mut families.metadata),
            };
            if let Some(tree) = tree {
                match &edit.value {
                    Some(value) => {
                        tree.insert(physical_key.to_vec(), value.clone());
                    }
                    None => {
                        tree.remove(physical_key);
                    }
                }
            }
        }
        State::Empty => unreachable!("engine state matches its selected layout"),
    }
    let _ = layout;
}

fn physical_key_into(layout: Layout, edit: &RecordEdit, scratch: &mut KeyScratch) {
    scratch.encoded.clear();
    match layout {
        Layout::UnifiedRowFirst => {
            encode_logical_prefix_into(&edit.key, &mut scratch.encoded);
            scratch.encoded.push(row_first_family_id(edit.namespace));
            scratch.encoded.push(KEY_FORMAT_VERSION);
            if let Some(timestamp) = edit.timestamp {
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
        }
        Layout::SeparateFamilies => match edit.namespace {
            Namespace::Default | Namespace::Write => {
                encode_logical_prefix_into(&edit.key, &mut scratch.encoded);
                scratch.encoded.push(KEY_FORMAT_VERSION);
                if let Some(timestamp) = edit.timestamp {
                    scratch
                        .encoded
                        .extend_from_slice(&(!timestamp).to_be_bytes());
                }
            }
            Namespace::Lock => {
                scratch.encoded.extend_from_slice(&edit.key);
                scratch.encoded.push(KEY_FORMAT_VERSION);
            }
            _ => {
                scratch.encoded.push(edit.namespace.id());
                encode_logical_prefix_into(&edit.key, &mut scratch.encoded);
                scratch.encoded.push(KEY_FORMAT_VERSION);
                if let Some(timestamp) = edit.timestamp {
                    scratch
                        .encoded
                        .extend_from_slice(&(!timestamp).to_be_bytes());
                }
            }
        },
        Layout::UnifiedFamilyFirst => {
            scratch.encoded.push(edit.namespace.id());
            encode_logical_prefix_into(&edit.key, &mut scratch.encoded);
            scratch.encoded.push(KEY_FORMAT_VERSION);
            if let Some(timestamp) = edit.timestamp {
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
        }
    }
}

fn row_first_family_id(namespace: Namespace) -> u8 {
    match namespace {
        Namespace::Write => 0x01,
        Namespace::Default => 0x02,
        Namespace::Lock => 0x03,
        _ => namespace.id(),
    }
}

fn encode_logical_prefix_into(logical_key: &[u8], output: &mut Vec<u8>) {
    for byte in logical_key {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

fn decode_logical_prefix(encoded: &[u8], offset: usize, output: &mut Vec<u8>) -> Option<usize> {
    output.clear();
    let mut index = offset;
    loop {
        let byte = *encoded.get(index)?;
        index += 1;
        if byte != 0 {
            output.push(byte);
            continue;
        }
        match *encoded.get(index)? {
            0 => return Some(index + 1),
            0xff => {
                output.push(0);
                index += 1;
            }
            _ => return None,
        }
    }
}

fn decode_timestamp(encoded: &[u8], suffix_offset: usize) -> Option<u64> {
    let bytes: [u8; 8] = encoded.get(suffix_offset..)?.try_into().ok()?;
    Some(!u64::from_be_bytes(bytes))
}

fn lookup_key_into(
    layout: Layout,
    namespace: Namespace,
    key: &[u8],
    timestamp: u64,
    scratch: &mut KeyScratch,
) {
    scratch.encoded.clear();
    match layout {
        Layout::UnifiedRowFirst => {
            encode_logical_prefix_into(key, &mut scratch.encoded);
            scratch.encoded.push(row_first_family_id(namespace));
            scratch.encoded.push(KEY_FORMAT_VERSION);
            if namespace != Namespace::Lock {
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
        }
        Layout::SeparateFamilies => match namespace {
            Namespace::Default | Namespace::Write => {
                encode_logical_prefix_into(key, &mut scratch.encoded);
                scratch.encoded.push(KEY_FORMAT_VERSION);
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
            Namespace::Lock => {
                scratch.encoded.extend_from_slice(key);
                scratch.encoded.push(KEY_FORMAT_VERSION);
            }
            _ => {
                scratch.encoded.push(namespace.id());
                encode_logical_prefix_into(key, &mut scratch.encoded);
                scratch.encoded.push(KEY_FORMAT_VERSION);
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
        },
        Layout::UnifiedFamilyFirst => {
            scratch.encoded.push(namespace.id());
            encode_logical_prefix_into(key, &mut scratch.encoded);
            scratch.encoded.push(KEY_FORMAT_VERSION);
            if namespace != Namespace::Lock {
                scratch
                    .encoded
                    .extend_from_slice(&(!timestamp).to_be_bytes());
            }
        }
    }
}

fn find_value<'a>(
    layout: Layout,
    state: &'a State,
    namespace: Namespace,
    key: &[u8],
    timestamp: u64,
    scratch: &mut KeyScratch,
) -> Option<&'a [u8]> {
    lookup_key_into(layout, namespace, key, timestamp, scratch);
    match state {
        State::Unified(unified) => unified.records.get(&scratch.encoded).map(Vec::as_slice),
        State::Separate(families) => match namespace {
            Namespace::Default => families.defaults.get(&scratch.encoded).map(Vec::as_slice),
            Namespace::Write => families.writes.get(&scratch.encoded).map(Vec::as_slice),
            Namespace::Lock => families.locks.get(&scratch.encoded).map(Vec::as_slice),
            _ => families.metadata.get(&scratch.encoded).map(Vec::as_slice),
        },
        State::Empty => None,
    }
}

fn write_seek_key(layout: Layout, key: &[u8], read_ts: u64, scratch: &mut KeyScratch) {
    lookup_key_into(layout, Namespace::Write, key, read_ts, scratch);
}

fn find_visible_write(
    layout: Layout,
    state: &State,
    key: &[u8],
    read_ts: u64,
    scratch: &mut KeyScratch,
) -> Option<WriteRecord> {
    write_seek_key(layout, key, read_ts, scratch);
    write_prefix_into(layout, key, scratch);
    match state {
        State::Unified(unified) => {
            let records = &unified.records;
            for (physical, value) in records.range(scratch.encoded.clone()..) {
                if !physical.starts_with(&scratch.prefix) {
                    break;
                }
                if decode_timestamp(physical, scratch.prefix.len() + 1)? > read_ts {
                    continue;
                }
                let Some(write) = decode_write_value(value) else {
                    continue;
                };
                if write.kind != WriteKind::Rollback {
                    return Some(write);
                }
            }
            None
        }
        State::Separate(families) => {
            for (physical, value) in families.writes.range(scratch.encoded.clone()..) {
                if !physical.starts_with(&scratch.prefix) {
                    break;
                }
                let Some(write) = decode_write_value(value) else {
                    continue;
                };
                if write.commit_ts > read_ts || write.kind == WriteKind::Rollback {
                    continue;
                }
                return Some(write);
            }
            None
        }
        State::Empty => None,
    }
}

fn write_prefix_into(layout: Layout, key: &[u8], scratch: &mut KeyScratch) {
    scratch.prefix.clear();
    match layout {
        Layout::UnifiedRowFirst => {
            encode_logical_prefix_into(key, &mut scratch.prefix);
            scratch.prefix.push(0x01);
        }
        Layout::SeparateFamilies => encode_logical_prefix_into(key, &mut scratch.prefix),
        Layout::UnifiedFamilyFirst => {
            scratch.prefix.push(Namespace::Write.id());
            encode_logical_prefix_into(key, &mut scratch.prefix);
        }
    }
}

fn scan_start_key(layout: Layout, start: &[u8], read_ts: u64, scratch: &mut KeyScratch) -> Vec<u8> {
    write_seek_key(layout, start, read_ts, scratch);
    scratch.encoded.clone()
}

fn next_row_cursor(layout: Layout, row: &[u8], cursor: &mut Vec<u8>) {
    cursor.clear();
    match layout {
        Layout::UnifiedRowFirst => {
            encode_logical_prefix_into(row, cursor);
            cursor.extend_from_slice(&[0xff; 16]);
        }
        Layout::SeparateFamilies => {
            encode_logical_prefix_into(row, cursor);
            cursor.push(KEY_FORMAT_VERSION);
            cursor.extend_from_slice(&[0xff; 16]);
        }
        Layout::UnifiedFamilyFirst => {
            cursor.push(Namespace::Write.id());
            encode_logical_prefix_into(row, cursor);
            cursor.push(KEY_FORMAT_VERSION);
            cursor.extend_from_slice(&[0xff; 16]);
        }
    }
}

/// Seeks each row's Write-family range directly instead of walking its
/// Default and Lock records. This is the row-first scan locality under test.
fn next_unified_candidate(
    layout: Layout,
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
    cursor: &[u8],
    read_ts: u64,
    scratch: &mut KeyScratch,
) -> Option<(Vec<u8>, WriteRecord)> {
    if layout == Layout::SeparateFamilies {
        return None;
    }
    let mut next_cursor = cursor.to_vec();
    loop {
        let (first_key, _) = records.range(next_cursor.clone()..).next()?;
        let logical_offset = usize::from(layout == Layout::UnifiedFamilyFirst);
        let family_offset = decode_logical_prefix(first_key, logical_offset, &mut scratch.logical)?;
        let row = scratch.logical.clone();
        let family = match layout {
            Layout::UnifiedRowFirst => *first_key.get(family_offset)?,
            Layout::UnifiedFamilyFirst => *first_key.first()?,
            Layout::SeparateFamilies => unreachable!(),
        };
        let write_family = match layout {
            Layout::UnifiedRowFirst => 0x01,
            Layout::UnifiedFamilyFirst => Namespace::Write.id(),
            Layout::SeparateFamilies => unreachable!(),
        };

        if family == write_family {
            scratch.prefix.clear();
            if layout == Layout::UnifiedFamilyFirst {
                scratch.prefix.push(Namespace::Write.id());
            }
            encode_logical_prefix_into(&row, &mut scratch.prefix);
            if layout == Layout::UnifiedRowFirst {
                scratch.prefix.push(write_family);
            }
            scratch.encoded.clear();
            scratch.encoded.extend_from_slice(&scratch.prefix);
            scratch.encoded.push(KEY_FORMAT_VERSION);
            scratch.encoded.extend_from_slice(&(!read_ts).to_be_bytes());

            for (physical, value) in records.range(scratch.encoded.clone()..) {
                if !physical.starts_with(&scratch.prefix) {
                    break;
                }
                let timestamp_offset = scratch.prefix.len() + 1;
                if decode_timestamp(physical, timestamp_offset).is_none_or(|ts| ts > read_ts) {
                    continue;
                }
                if let Some(write) = decode_write_value(value)
                    && write.kind != WriteKind::Rollback
                {
                    return Some((row, write));
                }
            }
        }

        next_row_cursor(layout, &row, &mut next_cursor);
    }
}

/// Advances one logical row at a time through Candidate B's ordered Write
/// tree, resolving each row from its first non-rollback visible write.
fn next_tree_candidate(
    writes: &BTreeMap<Vec<u8>, Vec<u8>>,
    cursor: &[u8],
    read_ts: u64,
    scratch: &mut KeyScratch,
) -> Option<(Vec<u8>, WriteRecord)> {
    let mut next_cursor = cursor.to_vec();
    loop {
        let (first_key, _) = writes.range(next_cursor.clone()..).next()?;
        decode_logical_prefix(first_key, 0, &mut scratch.logical)?;
        let row = scratch.logical.clone();
        scratch.prefix.clear();
        encode_logical_prefix_into(&row, &mut scratch.prefix);
        scratch.encoded.clear();
        scratch.encoded.extend_from_slice(&scratch.prefix);
        scratch.encoded.push(KEY_FORMAT_VERSION);
        scratch.encoded.extend_from_slice(&(!read_ts).to_be_bytes());

        for (physical, value) in writes.range(scratch.encoded.clone()..) {
            if !physical.starts_with(&scratch.prefix) {
                break;
            }
            if let Some(write) = decode_write_value(value)
                && write.commit_ts <= read_ts
                && write.kind != WriteKind::Rollback
            {
                return Some((row.clone(), write));
            }
        }

        next_row_cursor(Layout::SeparateFamilies, &row, &mut next_cursor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit_fixture() -> Vec<RecordEdit> {
        let mut edits = Vec::new();
        for id in 1..=8_i64 {
            let key = vec![0x01, id as u8];
            edits.push(RecordEdit::put_default(key.clone(), 1, &[id as u8; 4]));
            edits.push(RecordEdit::put_write(
                key.clone(),
                WriteRecord {
                    start_ts: 1,
                    commit_ts: 2,
                    kind: WriteKind::Put,
                },
            ));
            edits.push(RecordEdit::put_default(key.clone(), 3, &[id as u8 + 1; 4]));
            edits.push(RecordEdit::put_write(
                key.clone(),
                WriteRecord {
                    start_ts: 3,
                    commit_ts: 4,
                    kind: WriteKind::Put,
                },
            ));
            if id == 3 {
                edits.push(RecordEdit::put_write(
                    key.clone(),
                    WriteRecord {
                        start_ts: 8,
                        commit_ts: 8,
                        kind: WriteKind::Rollback,
                    },
                ));
            }
            if id == 5 {
                edits.push(RecordEdit::put_write(
                    key.clone(),
                    WriteRecord {
                        start_ts: 6,
                        commit_ts: 6,
                        kind: WriteKind::Delete,
                    },
                ));
            }
            if id == 7 {
                edits.push(RecordEdit::put_lock(key));
            }
        }
        edits
    }

    fn encoded_prefix(key: &[u8]) -> Vec<u8> {
        let mut prefix = Vec::new();
        encode_logical_prefix_into(key, &mut prefix);
        prefix
    }

    #[test]
    fn escaped_key_framing_preserves_canonical_order_and_descending_versions() {
        let keys = [vec![1], vec![1, 0], vec![1, 0, 2], vec![2], vec![0xff]];
        for pair in keys.windows(2) {
            assert_eq!(
                pair[0].cmp(&pair[1]),
                encoded_prefix(&pair[0]).cmp(&encoded_prefix(&pair[1]))
            );
        }
        let newer = (!300_u64).to_be_bytes();
        let older = (!200_u64).to_be_bytes();
        assert!(newer < older);
    }

    #[test]
    fn every_candidate_uses_explicit_experimental_key_and_value_versions() {
        let key = vec![0x02, 0x00, 0x03];
        let write = RecordEdit::put_write(
            key.clone(),
            WriteRecord {
                start_ts: 7,
                commit_ts: 8,
                kind: WriteKind::Put,
            },
        );
        let default = RecordEdit::put_default(key.clone(), 7, b"payload");
        let lock = RecordEdit::put_lock(key);
        let mut scratch = KeyScratch::default();
        let mut write_suffix = vec![KEY_FORMAT_VERSION];
        write_suffix.extend_from_slice(&(!8_u64).to_be_bytes());
        let mut default_suffix = vec![KEY_FORMAT_VERSION];
        default_suffix.extend_from_slice(&(!7_u64).to_be_bytes());

        for layout in [
            Layout::UnifiedRowFirst,
            Layout::SeparateFamilies,
            Layout::UnifiedFamilyFirst,
        ] {
            physical_key_into(layout, &write, &mut scratch);
            assert!(scratch.encoded.ends_with(&write_suffix));
            assert_eq!(
                write.value.as_ref().unwrap()[..2],
                [VALUE_FORMAT_VERSION, WRITE_VALUE_KIND]
            );

            physical_key_into(layout, &default, &mut scratch);
            assert!(scratch.encoded.ends_with(&default_suffix));
            assert_eq!(
                default.value.as_ref().unwrap()[..2],
                [VALUE_FORMAT_VERSION, DEFAULT_VALUE_KIND]
            );

            physical_key_into(layout, &lock, &mut scratch);
            assert!(scratch.encoded.ends_with(&[KEY_FORMAT_VERSION]));
            assert_eq!(lock.value.as_ref().unwrap().len(), LOCK_VALUE_BYTES);
        }
    }

    #[test]
    fn all_layouts_match_mvcc_visibility_rollback_delete_and_scan_semantics() {
        let edits = edit_fixture();
        let engines = [
            Engine::from_fixture(Layout::UnifiedRowFirst, edits.clone()).unwrap(),
            Engine::from_fixture(Layout::SeparateFamilies, edits.clone()).unwrap(),
            Engine::from_fixture(Layout::UnifiedFamilyFirst, edits).unwrap(),
        ];
        let keys = (1..=8).map(|id| vec![1, id]).collect::<Vec<_>>();

        for engine in &engines {
            let mut scratch = KeyScratch::default();
            assert_eq!(
                engine.read_at(&keys[1], 2, &mut scratch),
                Some(vec![2; 4]),
                "layout {:?}",
                engine.layout()
            );
            assert_eq!(engine.read_at(&keys[1], 4, &mut scratch), Some(vec![3; 4]));
            assert_eq!(engine.read_at(&keys[2], 8, &mut scratch), Some(vec![4; 4]));
            assert!(engine.has_rollback_witness(&keys[2], 8, &mut scratch));
            assert_eq!(engine.read_at(&keys[4], 6, &mut scratch), None);
            assert!(engine.contains_lock(&keys[6], &mut scratch));
            assert!(!engine.validate_prewrite(&keys[6], 10, &mut scratch));

            let rows = engine.scan_page(&keys[0], &keys[7], 4, 20, &mut scratch);
            let got = rows.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
            assert_eq!(got, keys[..7]);
            assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
        }
    }

    #[test]
    fn invalid_late_edit_leaves_every_layout_generation_unchanged() {
        for layout in [
            Layout::UnifiedRowFirst,
            Layout::SeparateFamilies,
            Layout::UnifiedFamilyFirst,
        ] {
            let engine = Engine::empty(layout);
            let valid = RecordEdit::put_default(vec![1, 1], 1, b"valid");
            let malformed = RecordEdit {
                key: vec![1, 2],
                namespace: Namespace::Default,
                timestamp: Some(1),
                value: Some(vec![0xff]),
            };
            let error = engine.publish_atomic(&[valid, malformed]).unwrap_err();
            assert_eq!(error, LayoutError::InvalidValue(Namespace::Default));
            assert_eq!(engine.record_count(), 0);
        }
    }

    #[test]
    fn duplicate_record_edits_are_rejected_before_any_layout_mutates() {
        for layout in [
            Layout::UnifiedRowFirst,
            Layout::SeparateFamilies,
            Layout::UnifiedFamilyFirst,
        ] {
            let engine = Engine::empty(layout);
            let first = RecordEdit::put_default(vec![1, 1], 1, b"first");
            let second = RecordEdit::put_default(vec![1, 1], 1, b"second");
            assert_eq!(
                engine.publish_atomic(&[first, second]).unwrap_err(),
                LayoutError::DuplicateRecord
            );
            assert_eq!(engine.record_count(), 0);
        }
    }

    #[test]
    fn cross_family_commit_replaces_the_lock_with_one_visible_committed_version() {
        for layout in [
            Layout::UnifiedRowFirst,
            Layout::SeparateFamilies,
            Layout::UnifiedFamilyFirst,
        ] {
            let engine = Engine::empty(layout);
            let key = vec![0x01, 0x2a];
            engine
                .publish_atomic(&[RecordEdit::put_lock(key.clone())])
                .unwrap();
            let mut scratch = KeyScratch::default();
            assert!(engine.contains_lock(&key, &mut scratch));
            assert_eq!(engine.read_at(&key, 8, &mut scratch), None);

            engine
                .publish_atomic(&[
                    RecordEdit::delete_lock(key.clone()),
                    RecordEdit::put_default(key.clone(), 7, b"committed"),
                    RecordEdit::put_write(
                        key.clone(),
                        WriteRecord {
                            start_ts: 7,
                            commit_ts: 8,
                            kind: WriteKind::Put,
                        },
                    ),
                ])
                .unwrap();

            assert!(!engine.contains_lock(&key, &mut scratch));
            assert_eq!(
                engine.read_at(&key, 8, &mut scratch),
                Some(b"committed".to_vec())
            );
        }
    }

    #[test]
    fn reserved_logical_namespaces_have_unique_stable_ids() {
        let ids = Namespace::ALL.map(Namespace::id);
        let unique = ids.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), unique.len());
        assert!(ids.contains(&Namespace::RetryOutcome.id()));
        assert!(ids.contains(&Namespace::TxnPrimaryStatus.id()));
        assert!(ids.contains(&Namespace::SecondaryIndex.id()));
        assert!(ids.contains(&Namespace::UniqueClaim.id()));

        for layout in [
            Layout::UnifiedRowFirst,
            Layout::SeparateFamilies,
            Layout::UnifiedFamilyFirst,
        ] {
            let engine = Engine::empty(layout);
            let metadata_edits = [
                Namespace::TxnPrimaryStatus,
                Namespace::RetryOutcome,
                Namespace::RetryFloor,
                Namespace::SecondaryIndex,
                Namespace::UniqueClaim,
            ]
            .into_iter()
            .map(|namespace| RecordEdit {
                key: vec![0x55, 0x66],
                namespace,
                timestamp: None,
                value: Some(vec![VALUE_FORMAT_VERSION, namespace.id()]),
            })
            .collect::<Vec<_>>();
            engine.publish_atomic(&metadata_edits).unwrap();
            assert_eq!(engine.record_count(), metadata_edits.len());
        }
    }

    #[test]
    fn both_unified_layouts_keep_each_family_contiguous_under_its_comparator() {
        let key = vec![0x01, 0x00, 0x02];
        let mut row_first_write = RecordEdit::put_write(
            key.clone(),
            WriteRecord {
                start_ts: 1,
                commit_ts: 2,
                kind: WriteKind::Put,
            },
        );
        let mut row_first_default = RecordEdit::put_default(key.clone(), 1, b"x");
        let mut family_first_write = row_first_write.clone();
        let mut family_first_default = row_first_default.clone();
        let mut scratch = KeyScratch::default();
        physical_key_into(Layout::UnifiedRowFirst, &row_first_write, &mut scratch);
        let a_write = scratch.encoded.clone();
        physical_key_into(Layout::UnifiedRowFirst, &row_first_default, &mut scratch);
        let a_default = scratch.encoded.clone();
        physical_key_into(
            Layout::UnifiedFamilyFirst,
            &family_first_write,
            &mut scratch,
        );
        let c_write = scratch.encoded.clone();
        physical_key_into(
            Layout::UnifiedFamilyFirst,
            &family_first_default,
            &mut scratch,
        );
        let c_default = scratch.encoded.clone();
        row_first_write.namespace = Namespace::Write;
        row_first_default.namespace = Namespace::Default;
        family_first_write.namespace = Namespace::Write;
        family_first_default.namespace = Namespace::Default;
        assert!(a_write < a_default);
        assert_eq!(c_write[0], Namespace::Write.id());
        assert_eq!(c_default[0], Namespace::Default.id());
        assert_ne!(c_write[0], c_default[0]);
    }
}
