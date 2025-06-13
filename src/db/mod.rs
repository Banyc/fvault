use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use redb::Database;
use tokio::task::spawn_blocking;

pub mod val;

pub fn read_table_result<K, V>(
    res: Result<redb::ReadOnlyTable<K, V>, redb::TableError>,
) -> anyhow::Result<Option<redb::ReadOnlyTable<K, V>>>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let table = match res {
        Ok(x) => x,
        Err(e) => panic!(
            "{:?}",
            match table_error_not_exist_error(table_error_storage_error(e)?) {
                Ok(x) => x,
                Err(_) => return Ok(None),
            }
        ),
    };
    Ok(Some(table))
}
pub fn write_table_result<K, V>(
    res: Result<redb::Table<K, V>, redb::TableError>,
) -> anyhow::Result<redb::Table<K, V>>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let table = match res {
        Ok(x) => x,
        Err(e) => panic!("{:?}", table_error_storage_error(e)?),
    };
    Ok(table)
}

#[derive(Debug)]
pub struct Db {
    db: Arc<RwLock<Database>>,
}
impl Db {
    pub async fn bind(index_db: PathBuf) -> anyhow::Result<Self> {
        spawn_blocking(move || -> anyhow::Result<Self> {
            let db = Database::builder().create(&index_db)?;
            let db = Arc::new(RwLock::new(db));
            Ok(Self { db })
        })
        .await
        .unwrap()
    }
    pub async fn read<F, O>(&self, blocking_f: F) -> anyhow::Result<O>
    where
        F: (FnOnce(&redb::ReadTransaction) -> anyhow::Result<O>) + Send + 'static,
        O: Send + 'static,
    {
        let db = self.db.clone();
        spawn_blocking(move || -> anyhow::Result<O> {
            let db = db.read().unwrap();
            let read = match db.begin_read() {
                Ok(x) => x,
                Err(e) => panic!("{:?}", transaction_error_storage_error(e)?),
            };
            blocking_f(&read)
        })
        .await
        .unwrap()
    }
    pub async fn write<F, O>(&mut self, blocking_f: F) -> anyhow::Result<O>
    where
        F: (FnOnce(&redb::WriteTransaction) -> anyhow::Result<(O, CloseWrite)>) + Send + 'static,
        O: Send + 'static,
    {
        let db = self.db.clone();
        spawn_blocking(move || -> anyhow::Result<O> {
            let db = db.write().unwrap();
            let write = match db.begin_write() {
                Ok(x) => x,
                Err(e) => panic!("{:?}", transaction_error_storage_error(e)?),
            };
            let (out, close) = blocking_f(&write)?;
            match close {
                CloseWrite::Commit => {
                    match write.commit() {
                        Ok(()) => (),
                        Err(e) => panic!("{:?}", commit_error_storage_error(e)?),
                    };
                }
                CloseWrite::Rollback => {
                    write.abort()?;
                }
            }
            Ok(out)
        })
        .await
        .unwrap()
    }
}
#[derive(Debug, Clone)]
pub enum CloseWrite {
    Commit,
    Rollback,
}

fn commit_error_storage_error(err: redb::CommitError) -> anyhow::Result<redb::CommitError> {
    match &err {
        redb::CommitError::Storage(_) => Err(err.into()),
        _ => Ok(err),
    }
}
fn transaction_error_storage_error(
    err: redb::TransactionError,
) -> anyhow::Result<redb::TransactionError> {
    match &err {
        redb::TransactionError::Storage(_) => Err(err.into()),
        _ => Ok(err),
    }
}
fn table_error_storage_error(err: redb::TableError) -> anyhow::Result<redb::TableError> {
    match &err {
        redb::TableError::Storage(_) => Err(err.into()),
        _ => Ok(err),
    }
}
fn table_error_not_exist_error(err: redb::TableError) -> anyhow::Result<redb::TableError> {
    match &err {
        redb::TableError::TableDoesNotExist(_) => Err(err.into()),
        _ => Ok(err),
    }
}
