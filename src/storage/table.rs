// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Typed wrappers over redb tables.

use redb::ReadableTable;
use rkyv::ser::serializers::AllocSerializer;
use smallvec::SmallVec;
use std::fmt;
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

const DATA_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");

/// Raw, untyped table access.
pub trait RawTable {
    /// Underlying redb database.
    fn db(&self) -> &Arc<redb::Database>;

    /// In-process sequence counter used by `UpdateWatcher`.
    fn seq_ctr(&self) -> &AtomicU64;

    /// Return the latest in-process sequence number of this table.
    fn last_seq(&self) -> u64 {
        self.seq_ctr().load(Relaxed)
    }

    /// Increase sequence number after successful write commit.
    fn bump_seq(&self) {
        self.seq_ctr().fetch_add(1, Relaxed);
    }

    /// Estimate number of records in this table.
    fn count_estimate(&self) -> u64 {
        count_entries(self.db()).unwrap_or(0)
    }

    /// Debug statistics string.
    fn rocksdb_statistics(&self) -> String {
        format!(
            "backend=redb keys={} seq={}",
            self.count_estimate(),
            self.last_seq()
        )
    }

    /// Pretty table name.
    fn pretty_name(&self) -> &'static str {
        table_name::<Self>()
    }
}

// Make sure `RawTable` remains object safe.
#[allow(unused)]
fn assert_raw_table_obj_safe(_: &dyn RawTable) {}

/// Typed database table.
pub trait Table: RawTable + Sized + From<Arc<redb::Database>> {
    /// Key format.
    type Key: TableKey;

    /// Value format.
    type Value: rkyv::Archive + rkyv::Serialize<AllocSerializer<4096>> + 'static;

    /// Removes the record with the given key.
    fn remove(&self, key: Self::Key) {
        let key_raw = key.into_raw();
        let write_txn = self.db().begin_write().expect("DB write begin failed");
        {
            let mut table = write_txn
                .open_table(DATA_TABLE)
                .expect("DB open table failed");
            table.remove(key_raw.as_ref()).expect("DB delete failed");
        }
        write_txn.commit().expect("DB write commit failed");
        self.bump_seq();
    }

    /// Inserts value at key.
    fn insert(&self, key: Self::Key, value: Self::Value) {
        let key_raw = key.into_raw();
        let value_bytes = serialize_value(&value);

        let write_txn = self.db().begin_write().expect("DB write begin failed");
        {
            let mut table = write_txn
                .open_table(DATA_TABLE)
                .expect("DB open table failed");
            table
                .insert(key_raw.as_ref(), value_bytes.as_ref())
                .expect("DB put failed");
        }
        write_txn.commit().expect("DB write commit failed");
        self.bump_seq();
    }

    /// Create a new insertion batch.
    fn batched_insert(&self) -> InsertionBatch<'_, Self> {
        InsertionBatch {
            table: self,
            rows: Vec::with_capacity(1024),
        }
    }

    /// Get value at key.
    fn get(&self, key: Self::Key) -> Option<TableValueRef<Self::Value, SmallVec<[u8; 64]>>> {
        let key_raw = key.into_raw();

        let read_txn = self.db().begin_read().expect("DB read begin failed");
        let table = read_txn
            .open_table(DATA_TABLE)
            .expect("DB open table failed");

        let value = table
            .get(key_raw.as_ref())
            .expect("DB read failed")?
            .value()
            .to_vec();

        Some(TableValueRef::new(SmallVec::from_vec(value)))
    }

    /// Checks whether key exists.
    fn contains_key(&self, key: Self::Key) -> bool {
        self.get(key).is_some()
    }

    /// Iterate over all key-value pairs.
    fn iter(&self) -> Iter<Self> {
        let read_txn = self.db().begin_read().expect("DB read begin failed");
        let table = read_txn
            .open_table(DATA_TABLE)
            .expect("DB open table failed");

        let mut out = Vec::new();
        let rows = table.iter().expect("DB iter failed");
        for row in rows {
            let (key, value) = row.expect("DB iter row failed");
            out.push(decode_row::<Self>(key.value(), value.value()));
        }

        Iter {
            raw: out.into_iter(),
        }
    }

    /// Iterate over key-value pairs in `[start, end)`.
    fn range(&self, start: Self::Key, end: Self::Key) -> Iter<Self> {
        let start_raw = start.into_raw();
        let end_raw = end.into_raw();

        let read_txn = self.db().begin_read().expect("DB read begin failed");
        let table = read_txn
            .open_table(DATA_TABLE)
            .expect("DB open table failed");

        let mut out = Vec::new();
        let rows = table
            .range(start_raw.as_ref()..end_raw.as_ref())
            .expect("DB range failed");
        for row in rows {
            let (key, value) = row.expect("DB range row failed");
            out.push(decode_row::<Self>(key.value(), value.value()));
        }

        Iter {
            raw: out.into_iter(),
        }
    }
}

fn count_entries(db: &redb::Database) -> anyhow::Result<u64> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(DATA_TABLE)?;

    let mut count = 0;
    for _ in table.iter()? {
        count += 1;
    }

    Ok(count)
}

fn serialize_value<T>(value: &T) -> SmallVec<[u8; 64]>
where
    T: rkyv::Archive + rkyv::Serialize<AllocSerializer<4096>>,
{
    let bytes = rkyv::to_bytes::<_, 4096>(value).expect("rkyv serialization failed");
    SmallVec::from_slice(bytes.as_ref())
}

fn decode_row<T: Table>(
    key_raw: &[u8],
    value_raw: &[u8],
) -> (T::Key, TableValueRef<T::Value, SmallVec<[u8; 64]>>) {
    let key_raw = match <T::Key as TableKey>::B::try_from(key_raw) {
        Ok(key_raw) => key_raw,
        Err(_) => panic!("bug: key size mismatch"),
    };
    let key = <T::Key as TableKey>::from_raw(key_raw);

    let value = SmallVec::from_slice(value_raw);
    let value = TableValueRef::new(value);

    (key, value)
}

/// Iterator over key-value pairs.
pub struct Iter<T: Table> {
    raw: std::vec::IntoIter<(T::Key, TableValueRef<T::Value, SmallVec<[u8; 64]>>)>,
}

impl<T: Table> Iterator for Iter<T> {
    type Item = (T::Key, TableValueRef<T::Value, SmallVec<[u8; 64]>>);

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next()
    }
}

impl<T: Table> FusedIterator for Iter<T> {}

/// Batched insertion helper.
pub struct InsertionBatch<'table, T: Table> {
    table: &'table T,
    rows: Vec<(T::Key, SmallVec<[u8; 64]>)>,
}

impl<T: Table> InsertionBatch<'_, T> {
    /// Add a record to the batch.
    pub fn insert(&mut self, key: T::Key, value: T::Value) {
        self.rows.push((key, serialize_value(&value)));
    }

    /// Atomically insert the batch.
    pub fn commit(self) {
        if self.rows.is_empty() {
            return;
        }

        let write_txn = self
            .table
            .db()
            .begin_write()
            .expect("DB write begin failed");
        {
            let mut table = write_txn
                .open_table(DATA_TABLE)
                .expect("DB open table failed");
            for (key, value) in self.rows {
                let key_raw = key.into_raw();
                table
                    .insert(key_raw.as_ref(), value.as_ref())
                    .expect("DB batch put failed");
            }
        }
        write_txn.commit().expect("DB write commit failed");
        self.table.bump_seq();
    }
}

impl<T: Table> fmt::Debug for InsertionBatch<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InsertionBatch(<{} records into {}>)",
            self.rows.len(),
            std::any::type_name::<T>(),
        )
    }
}

/// Type that can act as key for a table.
pub trait TableKey: 'static {
    /// Container type for raw key representation.
    type B: for<'a> TryFrom<&'a [u8]> + AsRef<[u8]>;

    /// Load raw key as typed value.
    fn from_raw(data: Self::B) -> Self;

    /// Store typed key as raw bytes.
    fn into_raw(self) -> Self::B;
}

/// Implements Rust ordering trait via the table key.
#[macro_export]
macro_rules! impl_ord_from_table_key {
    ($ty:ty) => {
        impl PartialOrd for $ty {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.into_raw().partial_cmp(&other.into_raw())
            }
        }

        impl Ord for $ty {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.into_raw().cmp(&other.into_raw())
            }
        }
    };
}

/// Reference to table value with lazy deserialization.
pub struct TableValueRef<T: rkyv::Archive, S: AsRef<[u8]>> {
    data: S,
    _marker: PhantomData<(T::Archived, S)>,
}

impl<T: rkyv::Archive, S: AsRef<[u8]>> TableValueRef<T, S> {
    /// Create a new table value reference.
    fn new(data: S) -> Self {
        Self {
            data,
            _marker: PhantomData,
        }
    }

    /// Borrowed access to archived data.
    pub fn get(&self) -> &T::Archived {
        unsafe { rkyv::archived_root::<T>(self.data.as_ref()) }
    }

    /// Deserialize into owned object.
    pub fn read(&self) -> T
    where
        <T as rkyv::Archive>::Archived:
            rkyv::Deserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        unsafe { rkyv::from_bytes_unchecked(self.data.as_ref()).unwrap() }
    }
}

/// Derive table name from type name.
fn table_name<T: ?Sized>() -> &'static str {
    let full = std::any::type_name::<T>();
    let name = full.rsplit_once("::").map(|x| x.1).unwrap();
    assert!(name.chars().all(|c| c.is_ascii_alphanumeric()));
    assert!(!name.is_empty());
    name
}

/// Convenience macro for defining a table wrapper.
#[macro_export]
macro_rules! new_table {
    ($name:ident: $key:ty => $value:ty $({ $($custom:tt)* })?) => {
        #[derive(::std::fmt::Debug)]
        pub struct $name {
            db: ::std::sync::Arc<::redb::Database>,
            seq: ::std::sync::atomic::AtomicU64,
        }

        impl $crate::storage::RawTable for $name {
            fn db(&self) -> &::std::sync::Arc<::redb::Database> {
                &self.db
            }

            fn seq_ctr(&self) -> &::std::sync::atomic::AtomicU64 {
                &self.seq
            }
        }

        impl $crate::storage::Table for $name {
            type Key = $key;
            type Value = $value;

            $($($custom)*)*
        }

        impl ::std::convert::From<::std::sync::Arc<::redb::Database>> for $name {
            fn from(db: ::std::sync::Arc<::redb::Database>) -> Self {
                Self {
                    db,
                    seq: ::std::sync::atomic::AtomicU64::new(0),
                }
            }
        }
    };
}

/// Open or create a table database in target directory.
pub fn open_or_create<T: Table>(dir: &Path) -> anyhow::Result<T> {
    let path = dir.join(format!("{}.redb", table_name::<T>()));

    let db = if path.exists() {
        redb::Database::open(&path)?
    } else {
        redb::Database::create(&path)?
    };

    // Ensure the data table exists.
    {
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(DATA_TABLE)?;
        }
        write_txn.commit()?;
    }

    Ok(T::from(Arc::new(db)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct TestKey(u64);

    impl TableKey for TestKey {
        type B = [u8; 8];

        fn from_raw(data: Self::B) -> Self {
            TestKey(u64::from_be_bytes(data))
        }

        fn into_raw(self) -> Self::B {
            self.0.to_be_bytes()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    pub struct TestValue {
        data: String,
    }

    new_table!(TestTable: TestKey => TestValue {});

    #[test]
    fn insert_get_remove_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table = open_or_create::<TestTable>(temp_dir.path()).unwrap();

        let key = TestKey(7);
        let value = TestValue {
            data: "abc".to_string(),
        };

        table.insert(key, value.clone());
        assert!(table.contains_key(key));
        assert_eq!(table.get(key).unwrap().read(), value);

        table.remove(key);
        assert!(!table.contains_key(key));
        assert!(table.get(key).is_none());
    }

    #[test]
    fn range_is_ordered_and_bounded() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table = open_or_create::<TestTable>(temp_dir.path()).unwrap();

        for i in 0..10 {
            table.insert(
                TestKey(i),
                TestValue {
                    data: format!("v{i}"),
                },
            );
        }

        let out: Vec<_> = table
            .range(TestKey(3), TestKey(7))
            .map(|(k, v)| (k.0, v.read().data))
            .collect();

        assert_eq!(
            out,
            vec![
                (3, "v3".to_string()),
                (4, "v4".to_string()),
                (5, "v5".to_string()),
                (6, "v6".to_string())
            ]
        );
    }

    #[test]
    fn batched_insert_works() {
        let temp_dir = tempfile::tempdir().unwrap();
        let table = open_or_create::<TestTable>(temp_dir.path()).unwrap();

        let mut batch = table.batched_insert();
        for i in 100..110 {
            batch.insert(
                TestKey(i),
                TestValue {
                    data: format!("b{i}"),
                },
            );
        }
        batch.commit();

        assert_eq!(table.count_estimate(), 10);
        assert_eq!(table.last_seq(), 1);
    }
}
