use std::io;

use anyhow::Context;
use async_compression::tokio::{bufread::BrotliDecoder, write::BrotliEncoder};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader},
};

use crate::{
    copy::{CopyConfig, copy_2},
    hash::{HashRead, HashStream, HashWrite},
    index::{
        InsertError, InterchangeStage, NewObjMeta, NextReloc, ObjHash, ObjLoc, ObjMetaKey, ShardId,
        ShardIterIndex, ShardIterKey, VaultIndex,
    },
    limit_read::LimitReader,
    path::CanPathBuf,
};

#[derive(Debug, Clone)]
pub struct StoreObjConfig {
    pub compress: bool,
}

pub async fn store_obj<ObjFile>(
    index: &mut VaultIndex,
    shard: ShardId,
    pack_file: &mut File,
    obj_path: CanPathBuf,
    obj_file: &mut ObjFile,
    config: &StoreObjConfig,
) -> anyhow::Result<Result<(), InsertError>>
where
    ObjFile: AsyncRead + Unpin + ?Sized,
{
    let shard_meta = index.ensure_shard_meta(shard).await?;
    let write_start_pos = shard_meta.write_pos;
    dbg!(write_start_pos);
    pack_file
        .seek(io::SeekFrom::Start(u64::try_from(write_start_pos).unwrap()))
        .await?;
    let mut hash_wtr = HashStream::new(&mut *pack_file, HashWrite);
    {
        let crypt_config = tokio_chacha20::stream::NonceCiphertextWriterConfig {
            key: &shard_meta.crypt_key.0,
            write_nonce: true,
            hash: false,
        };
        let nonce = tokio_chacha20::stream::NonceBuf::XNonce(Box::new(rand::random()));
        let mut crypt_wtr =
            tokio_chacha20::stream::NonceCiphertextWriter::new(&crypt_config, nonce, &mut hash_wtr);
        let copy_config = CopyConfig { limit: None };
        let res = if config.compress {
            let mut pack_wtr = BrotliEncoder::new(&mut crypt_wtr);
            let res = copy_2(obj_file, &mut pack_wtr, &copy_config).await;
            pack_wtr.flush().await?;
            pack_wtr.shutdown().await?;
            res
        } else {
            copy_2(obj_file, &mut crypt_wtr, &copy_config).await
        };
        res.res?;
    }
    let hash = hash_wtr.finalize();
    pack_file.sync_data().await?;
    let write_end_pos = usize::try_from(pack_file.stream_position().await?).unwrap();
    pack_file
        .set_len(u64::try_from(write_end_pos).unwrap())
        .await?;
    dbg!(write_end_pos);
    let len = write_end_pos - write_start_pos;
    let obj_meta = NewObjMeta {
        materialized_start_pos: write_start_pos,
        materialized_size: len,
        compressed: config.compress,
        hash: ObjHash(hash.into()),
    };
    if let Err(e) = index.insert_obj(shard, obj_path, obj_meta).await? {
        pack_file
            .set_len(u64::try_from(write_start_pos).unwrap())
            .await?;
        pack_file
            .seek(io::SeekFrom::Start(u64::try_from(write_start_pos).unwrap()))
            .await?;
        pack_file.sync_data().await?;
        return Ok(Err(e));
    };
    assert_eq!(
        index.shard_meta(shard).await?.unwrap().write_pos,
        write_end_pos
    );
    Ok(Ok(()))
}

pub async fn load_obj<ObjFile>(
    index: &VaultIndex,
    obj_meta_key: ObjMetaKey,
    pack_file: &mut File,
    interchange_file: &mut File,
    obj_file: &mut ObjFile,
) -> anyhow::Result<Result<(), LoadError>>
where
    ObjFile: AsyncWrite + Unpin + ?Sized,
{
    let Some(obj_meta) = index.obj_meta(obj_meta_key).await? else {
        return Ok(Err(LoadError::ObjNotExist));
    };
    let shard_meta = index.shard_meta(obj_meta.key.shard).await?.unwrap();
    let obj_meta = obj_meta.obj;
    let read_file = match obj_meta.materialized_pos {
        ObjLoc::Pack { start_pos } => {
            pack_file
                .seek(io::SeekFrom::Start(u64::try_from(start_pos).unwrap()))
                .await
                .context("seek")?;
            pack_file
        }
        ObjLoc::Interchange => {
            interchange_file
                .seek(io::SeekFrom::Start(0))
                .await
                .context("seek")?;
            interchange_file
        }
    };
    let mut hash_rdr = HashStream::new(&mut *read_file, HashRead);
    let config = CopyConfig { limit: None };
    let mut limit_read = LimitReader::new(&mut hash_rdr, obj_meta.materialized_size);
    let crypt_config = tokio_chacha20::stream::NonceCiphertextReaderConfig { hash: false };
    let nonce_buf =
        tokio_chacha20::stream::NonceBuf::XNonce(Box::new([0; tokio_chacha20::X_NONCE_BYTES]));
    let mut crypt_rdr = tokio_chacha20::stream::NonceCiphertextReader::new(
        &crypt_config,
        Box::new(*shard_meta.crypt_key.0),
        nonce_buf,
        &mut limit_read,
    );
    let res = if obj_meta.compressed {
        let mut buf_read = BufReader::new(&mut crypt_rdr);
        let mut read_rdr = BrotliDecoder::new(&mut buf_read);
        copy_2(&mut read_rdr, obj_file, &config).await
    } else {
        copy_2(&mut crypt_rdr, obj_file, &config).await
    };
    res.res.context("copy")?;
    let hash = hash_rdr.finalize();
    if hash != *obj_meta.hash.0 {
        return Ok(Err(LoadError::ObjIntegrity));
    }
    Ok(Ok(()))
}
#[derive(Debug, Clone, Error)]
pub enum LoadError {
    #[error("obj not exists")]
    ObjNotExist,
    #[error("obj bad integrity")]
    ObjIntegrity,
}

pub async fn defragment(
    index: &mut VaultIndex,
    shard: ShardId,
    pack_file: &mut File,
    interchange_file: &mut File,
) -> anyhow::Result<()> {
    let mut next_iter_start = ShardIterIndex(0);
    loop {
        let Some(shard_meta) = index.shard_meta(shard).await? else {
            return Ok(());
        };
        dbg!(&shard_meta.interchange_stage);
        match shard_meta.interchange_stage {
            None => {
                let Some(reloc) = index.find_next_reloc(shard, next_iter_start).await? else {
                    return Ok(());
                };
                match reloc {
                    NextReloc::Truncate(truncate) => {
                        let Some(truncate) = index.truncate(shard, truncate).await? else {
                            return Ok(());
                        };
                        pack_file
                            .set_len(u64::try_from(truncate.truncate_pos).unwrap())
                            .await?;
                        pack_file.sync_data().await?;
                        return Ok(());
                    }
                    NextReloc::Reloc(reloc) => {
                        next_iter_start = ShardIterIndex(reloc.gap.left_empty.0 + 1);
                        index.set_pack_to_interchange(shard, reloc).await?;
                    }
                }
            }
            Some(InterchangeStage::PackToInterchange(reloc)) => {
                let Some(obj_meta) = index
                    .obj_meta(ObjMetaKey::IterKey(ShardIterKey {
                        shard,
                        index: reloc.gap.right_some,
                    }))
                    .await?
                else {
                    return Ok(());
                };
                let pack_start_pos = match obj_meta.obj.materialized_pos {
                    ObjLoc::Pack { start_pos } => start_pos,
                    ObjLoc::Interchange => return Ok(()),
                };
                pack_file
                    .seek(io::SeekFrom::Start(u64::try_from(pack_start_pos).unwrap()))
                    .await?;
                interchange_file.seek(io::SeekFrom::Start(0)).await?;
                let config = CopyConfig {
                    limit: Some(obj_meta.obj.materialized_size),
                };
                let res = copy_2(pack_file, interchange_file, &config).await;
                res.res?;
                interchange_file.sync_data().await?;
                index.set_interchange_to_pack(shard, reloc).await?;
            }
            Some(InterchangeStage::InterchangeToPack(reloc)) => {
                let Some(obj_meta) = index
                    .obj_meta(ObjMetaKey::IterKey(ShardIterKey {
                        shard,
                        index: reloc.gap.right_some,
                    }))
                    .await?
                else {
                    return Ok(());
                };
                match obj_meta.obj.materialized_pos {
                    ObjLoc::Pack { start_pos: _ } => return Ok(()),
                    ObjLoc::Interchange => (),
                };
                pack_file
                    .seek(io::SeekFrom::Start(
                        u64::try_from(reloc.dst_pack_pos).unwrap(),
                    ))
                    .await?;
                interchange_file.seek(io::SeekFrom::Start(0)).await?;
                let config = CopyConfig {
                    limit: Some(obj_meta.obj.materialized_size),
                };
                let res = copy_2(interchange_file, pack_file, &config).await;
                res.res?;
                interchange_file.set_len(0).await?;
                pack_file.sync_data().await?;
                index.clear_interchange(shard, reloc).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::{fs::create_dir_all, io::AsyncReadExt};

    use crate::{
        cx::{VaultConfig, init_vault_cx},
        index::ObjMetaKey,
        path::canonicalize_path,
    };

    use super::*;

    #[tokio::test]
    async fn store_load() {
        let dir = "./target/test";
        create_dir_all(dir).await.unwrap();
        let config = VaultConfig {
            work_dir: dir.into(),
            shard_count: 2,
        };
        let mut vault = init_vault_cx(&config).await.unwrap();
        let src_path = "./Cargo.toml";
        let src_path = canonicalize_path(src_path).unwrap();
        let mut src_bytes = vec![];
        {
            let mut src_file = File::options()
                .read(true)
                .open("./Cargo.toml")
                .await
                .unwrap();
            src_file.read_to_end(&mut src_bytes).await.unwrap();
        }
        let mut src_rdr = io::Cursor::new(&src_bytes[..]);
        let shard_id = ShardId(0);
        vault.index.remove_obj(src_path.clone()).await.unwrap();
        let config = StoreObjConfig { compress: true };
        store_obj(
            &mut vault.index,
            shard_id,
            &mut vault.shards[0].pack,
            src_path.clone(),
            &mut src_rdr,
            &config,
        )
        .await
        .unwrap()
        .unwrap();
        let mut dst_bytes = vec![];
        let shard = &mut vault.shards[0];
        let src_key = ObjMetaKey::Path(src_path);
        load_obj(
            &vault.index,
            src_key.clone(),
            &mut shard.pack,
            &mut shard.interchange,
            &mut dst_bytes,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(src_bytes, dst_bytes);
        let children = vault
            .index
            .list(canonicalize_path("/").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(children, ["Cargo.toml"]);
        defragment(
            &mut vault.index,
            shard_id,
            &mut shard.pack,
            &mut shard.interchange,
        )
        .await
        .unwrap();
        dst_bytes.clear();
        load_obj(
            &vault.index,
            src_key.clone(),
            &mut shard.pack,
            &mut shard.interchange,
            &mut dst_bytes,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(src_bytes, dst_bytes);
        let shard_meta = vault.index.shard_meta(shard_id).await.unwrap().unwrap();
        assert_eq!(shard_meta.next_iter_index.0, 1);
        if !config.compress {
            assert_eq!(shard_meta.write_pos, src_bytes.len());
        }
    }
}
