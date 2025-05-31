use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;

use crate::{cx::VaultCx, path::canonicalize_path};

#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    pub path: Option<PathBuf>,
}
pub async fn exec_list(
    args: ListArgs,
    vault: &mut VaultCx,
    pwd_rel_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let path = args.path.unwrap_or("".into());
    let obj_path = pwd_rel_path.as_ref().join(path);
    let obj_path =
        canonicalize_path(&obj_path).with_context(|| format!("canonicalize path {obj_path:?}"))?;
    let ls = vault.index.list(obj_path).await?;
    dbg!(ls);
    Ok(())
}
