use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use tokio::fs::{File, create_dir_all, metadata, remove_file, try_exists};

use crate::{
    cli::path::visit_files,
    cx::VaultCx,
    index::{ObjMetaKey, ShardId, VaultIndex},
    pack::{StoreObjConfig, defragment, load_obj, store_obj},
    path::{CanPathBuf, canonicalize_path},
};

#[derive(Debug, Clone, Args)]
pub struct StoreArgs {
    pub paths: Vec<PathBuf>,
    #[clap(long, short)]
    pub compress: bool,
    #[clap(long, short)]
    pub recursive: bool,
}
pub async fn exec_store(
    args: StoreArgs,
    vault: &mut VaultCx,
    pwd_rel_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let config = StoreObjConfig {
        compress: args.compress,
    };
    for path in args.paths {
        let shard_id = ShardId(0);
        let shard = &mut vault.shards[shard_id.0];
        let mut store = async |index: &mut VaultIndex, path: PathBuf| -> anyhow::Result<()> {
            let obj_path = pwd_rel_path.as_ref().join(&path);
            let obj_path = canonicalize_path(&obj_path)
                .with_context(|| format!("canonicalize path {obj_path:?}"))?;
            let mut obj_file = File::options()
                .read(true)
                .open(&path)
                .await
                .with_context(|| format!("{path:?}"))?;
            store_obj(
                index,
                shard_id,
                &mut shard.pack,
                obj_path,
                &mut obj_file,
                &config,
            )
            .await??;
            Ok(())
        };
        match args.recursive {
            true => {
                let paths = visit_files(&path).await?;
                for path in paths {
                    store(&mut vault.index, path).await?;
                }
            }
            false => {
                store(&mut vault.index, path).await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Args)]
pub struct LoadArgs {
    pub paths: Vec<PathBuf>,
    #[clap(long, short)]
    pub recursive: bool,
}
pub async fn exec_load(
    args: LoadArgs,
    vault: &mut VaultCx,
    pwd_rel_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    for path in args.paths {
        let shard_id = ShardId(0);
        let shard = &mut vault.shards[shard_id.0];
        if try_exists(&path).await? {
            let path_meta = metadata(&path).await?;
            if path_meta.is_file() {
                println!("{path:?} exist");
                continue;
            }
        }
        if let Some(parent) = path.parent() {
            create_dir_all(parent).await?;
        }
        let mut load = async |path: PathBuf, obj_path: CanPathBuf| -> anyhow::Result<()> {
            if let Some(parent) = path.parent() {
                create_dir_all(parent).await?;
            }
            let mut obj_file = File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await
                .with_context(|| format!("{path:?}"))?;
            let obj_meta_key = ObjMetaKey::Path(obj_path);
            if let Err(e) = load_obj(
                &vault.index,
                obj_meta_key,
                &mut shard.pack,
                &mut shard.interchange,
                &mut obj_file,
            )
            .await?
            {
                println!("{path:?} {e:?}");
                remove_file(&path).await?;
            }
            Ok(())
        };
        match args.recursive {
            true => {
                let node_path = pwd_rel_path.as_ref().join(&path);
                let node_path = canonicalize_path(&node_path)
                    .with_context(|| format!("canonicalize path {node_path:?}"))?;
                for obj_path in vault.index.visit_objs(node_path.clone()).await? {
                    let path = path.join(obj_path.subtract(&node_path).unwrap());
                    load(path, obj_path).await?;
                }
            }
            false => {
                let obj_path = pwd_rel_path.as_ref().join(&path);
                let obj_path = canonicalize_path(&obj_path)
                    .with_context(|| format!("canonicalize path {obj_path:?}"))?;
                load(path, obj_path).await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Args)]
pub struct GcArgs {}
pub async fn exec_gc(_args: GcArgs, vault: &mut VaultCx) -> anyhow::Result<()> {
    let shard_id = ShardId(0);
    let shard = &mut vault.shards[shard_id.0];
    defragment(
        &mut vault.index,
        shard_id,
        &mut shard.pack,
        &mut shard.interchange,
    )
    .await?;
    Ok(())
}
