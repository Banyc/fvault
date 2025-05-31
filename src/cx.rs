use std::path::PathBuf;

use anyhow::Context;
use tokio::fs::{File, try_exists};

use crate::{cli::db::ENCRYPTED_DB_NAME, index::VaultIndex};

#[derive(Debug, Clone)]
pub struct VaultConfig {
    pub work_dir: PathBuf,
    pub shard_count: usize,
}

pub const INDEX_DB_NAME: &str = "index.redb";

pub async fn init_vault_cx(config: &VaultConfig) -> anyhow::Result<VaultCx> {
    let index_db = config.work_dir.join(INDEX_DB_NAME);
    if try_exists(config.work_dir.join(ENCRYPTED_DB_NAME)).await? {
        return Err(anyhow::anyhow!("encrypted"));
    }
    let index = VaultIndex::bind(index_db.clone())
        .await
        .with_context(|| format!("{index_db:?}"))?;
    let mut shards = vec![];
    for i in 0..config.shard_count {
        let pack_path = config.work_dir.join(format!("shard.{i}.pack"));
        let interchange_path = config.work_dir.join(format!("shard.{i}.interchange"));
        let pack = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&pack_path)
            .await
            .with_context(|| format!("{pack_path:?}"))?;
        let interchange = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&interchange_path)
            .await
            .with_context(|| format!("{interchange_path:?}"))?;
        shards.push(ShardCx { pack, interchange });
    }
    Ok(VaultCx { index, shards })
}

#[derive(Debug)]
pub struct VaultCx {
    pub index: VaultIndex,
    pub shards: Vec<ShardCx>,
}
#[derive(Debug)]
pub struct ShardCx {
    pub pack: File,
    pub interchange: File,
}
