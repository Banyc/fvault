use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;

use crate::{cx::VaultCx, path::canonicalize_path};

#[derive(Debug, Clone, Args)]
pub struct RemoveArgs {
    pub paths: Vec<PathBuf>,
    #[clap(long, short)]
    pub recursive: bool,
}
pub async fn exec_remove(
    args: RemoveArgs,
    vault: &mut VaultCx,
    pwd_rel_path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    for path in args.paths {
        let path = pwd_rel_path.as_ref().join(&path);
        let path =
            canonicalize_path(&path).with_context(|| format!("canonicalize path {path:?}"))?;
        match args.recursive {
            true => {
                vault.index.remove_node(path).await?;
            }
            false => {
                vault.index.remove_obj(path).await?;
            }
        }
    }
    Ok(())
}
