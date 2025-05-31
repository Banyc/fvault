use std::{
    io::{Read, Write},
    path::Path,
};

use anyhow::Context;
use clap::Args;
use tokio::{
    fs::{File, try_exists},
    io::{AsyncReadExt, AsyncWriteExt},
    task::spawn_blocking,
};
use tokio_chacha20::{
    KEY_BYTES, X_NONCE_BYTES,
    mac::BLOCK_BYTES,
    stream::{
        ChaCha20ReadStateConfig, ChaCha20Reader, ChaCha20ReaderConfig, ChaCha20WriteStateConfig,
        ChaCha20Writer, ChaCha20WriterConfig, NonceBuf,
    },
};

use crate::{cli::path::REPO_DIR_NAME, cx::INDEX_DB_NAME, limit_read::LimitReader};

pub const ENCRYPTED_DB_NAME: &str = "index.nah";
pub const DECRYPTING_DB_NAME: &str = "index.tmp";

#[derive(Debug, Clone, Args)]
pub struct EncryptArgs {}
pub async fn exec_encrypt(_args: EncryptArgs, repo_base: impl AsRef<Path>) -> anyhow::Result<()> {
    let repo_path = repo_base.as_ref().join(REPO_DIR_NAME);
    let db_src_path = repo_path.join(INDEX_DB_NAME);
    let db_dst_path = repo_path.join(ENCRYPTED_DB_NAME);
    let mut db_src = File::options()
        .read(true)
        .open(&db_src_path)
        .await
        .with_context(|| anyhow::anyhow!("{db_src_path:?}"))?;
    let mut db_dst = File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&db_dst_path)
        .await
        .with_context(|| anyhow::anyhow!("{db_dst_path:?}"))?;
    let key = read_key_from_stdin().await?;
    println!("{key:?}");
    let len = db_src.metadata().await?.len();
    let nonce: [u8; X_NONCE_BYTES] = rand::random();
    db_dst.write_all(&nonce).await?;
    let nonce = NonceBuf::XNonce(Box::new(nonce));
    let config = ChaCha20WriterConfig {
        state: &ChaCha20WriteStateConfig {
            key: &key,
            nonce: &nonce,
            hash: true,
        },
    };
    let mut db_dst_en = ChaCha20Writer::new(&config, &mut db_dst);
    db_dst_en.write_all(&len.to_le_bytes()).await?;
    tokio::io::copy(&mut db_src, &mut db_dst_en).await?;
    let (_, hasher) = db_dst_en.into_inner();
    let tag = hasher.unwrap().finalize();
    db_dst.write_all(&tag).await?;
    tokio::fs::remove_file(&db_src_path).await?;
    Ok(())
}

#[derive(Debug, Clone, Args)]
pub struct DecryptArgs {}
pub async fn exec_decrypt(args: DecryptArgs, repo_base: impl AsRef<Path>) -> anyhow::Result<()> {
    let repo_path = repo_base.as_ref().join(REPO_DIR_NAME);
    let db_dst_path = repo_path.join(INDEX_DB_NAME);
    if try_exists(&db_dst_path).await? {
        return Err(anyhow::anyhow!("already decrypted"));
    }
    exec_decrypt_1(args, repo_base.as_ref()).await?;
    let db_src_path = repo_path.join(DECRYPTING_DB_NAME);
    tokio::fs::rename(&db_src_path, &db_dst_path).await?;
    let encrypted_db_path = repo_path.join(ENCRYPTED_DB_NAME);
    tokio::fs::remove_file(&encrypted_db_path).await?;
    Ok(())
}
async fn exec_decrypt_1(_args: DecryptArgs, repo_base: impl AsRef<Path>) -> anyhow::Result<()> {
    let repo_path = repo_base.as_ref().join(REPO_DIR_NAME);
    let db_dst_path = repo_path.join(DECRYPTING_DB_NAME);
    let db_src_path = repo_path.join(ENCRYPTED_DB_NAME);
    let mut db_src = File::options()
        .read(true)
        .open(&db_src_path)
        .await
        .with_context(|| anyhow::anyhow!("{db_src_path:?}"))?;
    let mut db_dst = File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&db_dst_path)
        .await
        .with_context(|| anyhow::anyhow!("{db_dst_path:?}"))?;
    let key = read_key_from_stdin().await?;
    println!("{key:?}");
    let mut nonce = [0; X_NONCE_BYTES];
    db_src.read_exact(&mut nonce).await?;
    let config = ChaCha20ReaderConfig {
        state: &ChaCha20ReadStateConfig {
            key: &key,
            nonce: &NonceBuf::XNonce(Box::new(nonce)),
            hash: true,
        },
    };
    let mut db_src_de = ChaCha20Reader::new(&config, &mut db_src);
    let mut len = 0_u64.to_le_bytes();
    db_src_de.read_exact(&mut len).await?;
    let len = u64::from_le_bytes(len);
    let len = usize::try_from(len)?;
    let mut db_src_limited = LimitReader::new(&mut db_src_de, len);
    tokio::io::copy(&mut db_src_limited, &mut db_dst).await?;
    let (_, hasher) = db_src_de.into_inner();
    let expected_tag = hasher.unwrap().finalize();
    let mut tag = [0; BLOCK_BYTES];
    db_src.read_exact(&mut tag).await?;
    if tag != expected_tag {
        return Err(anyhow::anyhow!("wrong key or file corrupted"));
    }
    Ok(())
}

async fn read_key_from_stdin() -> anyhow::Result<[u8; KEY_BYTES]> {
    spawn_blocking(move || {
        {
            let mut stdout = std::io::stdout().lock();
            stdout.write_all(b"type in seed: ")?;
            stdout.flush()?;
        }
        let mut seed = String::new();
        loop {
            let char = {
                let mut stdin = std::io::stdin().lock();
                let mut byte = [0];
                stdin.read_exact(&mut byte)?;
                byte[0]
            };
            let char = char::from(char);
            if char == '\n' {
                break;
            }
            if char == '\x08' {
                seed.pop();
                continue;
            }
            seed.push(char);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(seed.as_bytes());
        Ok(*hasher.finalize().as_bytes())
    })
    .await
    .unwrap()
}
