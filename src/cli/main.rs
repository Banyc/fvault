use std::path::PathBuf;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use tokio::fs::create_dir_all;

use crate::{
    cli::{
        db, index, pack,
        path::{REPO_DIR_NAME, repo_rel_path},
        tree,
    },
    cx::{VaultConfig, init_vault_cx},
};

#[derive(Debug, Clone, Parser)]
pub struct MainArgs {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// create a vault repo
    Init(InitArgs),
    /// store files to vault
    Store(pack::StoreArgs),
    /// load files from vault
    Load(pack::LoadArgs),
    /// defragment pack files
    Gc(pack::GcArgs),
    /// mark deletion for files in vault
    Rm(index::RemoveArgs),
    /// list files in vault
    Ls(tree::ListArgs),
    /// encrypt vault repo
    En(db::EncryptArgs),
    /// decrypt vault repo
    De(db::DecryptArgs),
}
pub async fn exec_main(args: MainArgs) -> anyhow::Result<()> {
    let command = match args.command {
        Command::Init(init_args) => {
            exec_init(init_args).await?;
            return Ok(());
        }
        x => x,
    };
    let Some(pwd) = repo_rel_path(".").await? else {
        return Err(anyhow::anyhow!("repo not found"));
    };
    let command = match command {
        Command::En(args) => {
            db::exec_encrypt(args, &pwd.repo_base).await?;
            return Ok(());
        }
        Command::De(args) => {
            db::exec_decrypt(args, &pwd.repo_base).await?;
            return Ok(());
        }
        x => x,
    };
    let config = VaultConfig {
        work_dir: pwd.repo_base.join(REPO_DIR_NAME),
        shard_count: 2,
    };
    let mut vault = init_vault_cx(&config).await?;
    match command {
        Command::Init(_) => panic!(),
        Command::Store(args) => pack::exec_store(args, &mut vault, pwd.rel_path).await?,
        Command::Load(args) => pack::exec_load(args, &mut vault, pwd.rel_path).await?,
        Command::Gc(args) => pack::exec_gc(args, &mut vault).await?,
        Command::Rm(remove_args) => {
            index::exec_remove(remove_args, &mut vault, pwd.rel_path).await?
        }
        Command::Ls(list_args) => tree::exec_list(list_args, &mut vault, pwd.rel_path).await?,
        Command::En(_) => panic!(),
        Command::De(_) => panic!(),
    }
    Ok(())
}

#[derive(Debug, Clone, Args)]
pub struct InitArgs {
    pub path: Option<PathBuf>,
}
pub async fn exec_init(args: InitArgs) -> anyhow::Result<()> {
    let repo_base = args.path.unwrap_or(".".into());
    let repo_dir = repo_base.join(REPO_DIR_NAME);
    create_dir_all(&repo_dir)
        .await
        .with_context(|| format!("{repo_dir:?}"))?;
    let config = VaultConfig {
        work_dir: repo_dir,
        shard_count: 2,
    };
    init_vault_cx(&config).await?;
    Ok(())
}
