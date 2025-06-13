use std::path::PathBuf;

use bincode::{Decode, Encode};
use redb::{ReadableTable, TableDefinition};
use thiserror::Error;

use crate::{
    db::{CloseWrite, Db, read_table_result, val::Bincode, write_table_result},
    path::CanPathBuf,
};

mod tree;

#[derive(Debug)]
pub struct VaultIndex {
    db: Db,
}
impl VaultIndex {
    pub async fn bind(index_db: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            db: Db::bind(index_db).await?,
        })
    }
    pub async fn list(&self, path: CanPathBuf) -> anyhow::Result<Option<Vec<String>>> {
        self.db
            .read(move |read| match tree::blocking_list(read, path)? {
                Some(node) => match node {
                    tree::Node::NonLeaf(non_leaf_node) => Ok(Some(non_leaf_node.children)),
                    tree::Node::Leaf(_) => Ok(None),
                },
                None => Ok(None),
            })
            .await
    }
    pub async fn ensure_shard_meta(&mut self, shard_id: ShardId) -> anyhow::Result<ShardMeta> {
        self.db
            .write(move |write| {
                let mut table = write_table_result(write.open_table(SHARD_META_TABLE))?;
                if table.get(&shard_id)?.is_none() {
                    table.insert(shard_id, new_shard_meta())?;
                }
                Ok((table.get(shard_id)?.unwrap().value(), CloseWrite::Commit))
            })
            .await
    }
    pub async fn shard_meta(&self, shard_id: ShardId) -> anyhow::Result<Option<ShardMeta>> {
        self.db
            .read(move |read| {
                let Some(table) = read_table_result(read.open_table(SHARD_META_TABLE))? else {
                    return Ok(None);
                };
                let Some(shard) = table.get(&shard_id)? else {
                    return Ok(None);
                };
                Ok(Some(shard.value()))
            })
            .await
    }
    pub async fn obj_meta(&self, key: ObjMetaKey) -> anyhow::Result<Option<ObjIndexMeta>> {
        self.db
            .read(move |read| {
                let obj_path = match key {
                    ObjMetaKey::IterKey(shard_iter_key) => {
                        let Some(table) = read_table_result(read.open_table(SHARD_ITER_TABLE))?
                        else {
                            return Ok(None);
                        };
                        let Some(path) = table.get(&shard_iter_key)? else {
                            return Ok(None);
                        };
                        path.value()
                    }
                    ObjMetaKey::Path(can_path_buf) => can_path_buf,
                };
                let Some(table) = read_table_result(read.open_table(OBJ_META_TABLE))? else {
                    return Ok(None);
                };
                let Some(shard) = table.get(&obj_path)? else {
                    return Ok(None);
                };
                Ok(Some(shard.value().clone()))
            })
            .await
    }
    pub async fn visit_objs(&self, node_path: CanPathBuf) -> anyhow::Result<Vec<CanPathBuf>> {
        self.db
            .read(move |read| {
                let mut objs = vec![];
                for (path, node) in tree::blocking_visit(read, node_path)? {
                    match node {
                        tree::Node::NonLeaf(_) => continue,
                        tree::Node::Leaf(_) => (),
                    }
                    objs.push(path);
                }
                Ok(objs)
            })
            .await
    }
    pub async fn remove_node(&mut self, node_path: CanPathBuf) -> anyhow::Result<()> {
        self.db
            .write(move |write| {
                for (path, node) in tree::blocking_remove(write, node_path)? {
                    match node {
                        tree::Node::NonLeaf(_) => continue,
                        tree::Node::Leaf(_) => (),
                    }
                    blocking_remove_obj(write, path)?.unwrap();
                }
                Ok(((), CloseWrite::Commit))
            })
            .await
    }
    pub async fn remove_obj(
        &mut self,
        obj_path: CanPathBuf,
    ) -> anyhow::Result<Option<ObjIndexMeta>> {
        self.db
            .write(move |write| {
                let Some(removed_obj) = blocking_remove_obj(write, obj_path.clone())? else {
                    return Ok((None, CloseWrite::Rollback));
                };
                tree::blocking_remove(write, obj_path)?;
                Ok((Some(removed_obj), CloseWrite::Commit))
            })
            .await
    }
    pub async fn rename_obj(
        &mut self,
        obj_path: CanPathBuf,
        new_obj_path: CanPathBuf,
    ) -> anyhow::Result<Result<(), RenameError>> {
        self.db
            .write(move |write| {
                tree::blocking_remove(write, obj_path.clone())?;
                if let Err(e) = tree::blocking_insert(write, new_obj_path.clone())? {
                    match e {
                        tree::InsertError::Blocked { by } => {
                            return Ok((Err(RenameError::Blocked { by }), CloseWrite::Rollback));
                        }
                    }
                };
                let _ = blocking_remove_obj(write, new_obj_path.clone())?;
                let mut table = write_table_result(write.open_table(OBJ_META_TABLE))?;
                let obj_meta = {
                    let Some(obj_meta) = table.remove(&obj_path)? else {
                        return Ok((Err(RenameError::SourceNotExist), CloseWrite::Rollback));
                    };
                    obj_meta.value().clone()
                };
                let shard_iter_key = obj_meta.key;
                assert!(table.insert(new_obj_path.clone(), obj_meta)?.is_none());
                let mut table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
                assert!(table.insert(shard_iter_key, new_obj_path)?.is_none());
                Ok((Ok(()), CloseWrite::Commit))
            })
            .await
    }
    pub async fn insert_obj(
        &mut self,
        shard_id: ShardId,
        obj_path: CanPathBuf,
        obj_meta: NewObjMeta,
    ) -> anyhow::Result<Result<(), InsertError>> {
        self.db
            .write(move |write| {
                if let Err(e) = tree::blocking_insert(write, obj_path.clone())? {
                    match e {
                        tree::InsertError::Blocked { by } => {
                            return Ok((Err(InsertError::Blocked { by }), CloseWrite::Rollback));
                        }
                    }
                };
                let _ = blocking_remove_obj(write, obj_path.clone())?;
                let mut table = write_table_result(write.open_table(SHARD_META_TABLE))?;
                if table.get(&shard_id)?.is_none() {
                    table.insert(shard_id, new_shard_meta())?;
                }
                let shard = table.get(&shard_id)?.unwrap();
                let next_iter_index = shard.value().next_iter_index;
                let mut new_shard = (shard.value()).clone();
                drop(shard);
                new_shard.write_pos = obj_meta.materialized_start_pos + obj_meta.materialized_size;
                new_shard.next_iter_index = ShardIterIndex(next_iter_index.0 + 1);
                table.insert(shard_id, new_shard)?;
                let mut table = write_table_result(write.open_table(OBJ_META_TABLE))?;
                let shard_iter_key = ShardIterKey {
                    shard: shard_id,
                    index: next_iter_index,
                };
                let file_index_meta = ObjIndexMeta {
                    key: shard_iter_key,
                    obj: convert_obj_meta(obj_meta),
                };
                table.insert(obj_path.clone(), file_index_meta)?;
                let mut table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
                table.insert(shard_iter_key, obj_path)?;
                Ok((Ok(()), CloseWrite::Commit))
            })
            .await
    }
    pub async fn find_next_reloc(
        &self,
        shard_id: ShardId,
        start: ShardIterIndex,
    ) -> anyhow::Result<Option<NextReloc>> {
        self.db
            .read(move |read| {
                let Some(table) = read_table_result(read.open_table(SHARD_META_TABLE))? else {
                    return Ok(None);
                };
                let Some(shard_meta) = table.get(shard_id)? else {
                    return Ok(None);
                };
                let iter_end = shard_meta.value().next_iter_index;
                let Some(table) = read_table_result(read.open_table(SHARD_ITER_TABLE))? else {
                    return Ok(None);
                };
                let mut left_empty = None;
                let mut dst_pack_pos = None;
                for i in start.0..iter_end.0 {
                    let a = table.get(ShardIterKey {
                        shard: shard_id,
                        index: ShardIterIndex(i),
                    })?;
                    if a.is_none() && left_empty.is_none() {
                        left_empty = Some(ShardIterIndex(i));
                        dst_pack_pos = Some(if let Some(prev) = i.checked_sub(1) {
                            let prev = ShardIterIndex(prev);
                            let Some(prev) = table.get(ShardIterKey {
                                shard: shard_id,
                                index: prev,
                            })?
                            else {
                                return Ok(None);
                            };
                            let prev_path = prev.value();
                            let table =
                                read_table_result(read.open_table(OBJ_META_TABLE))?.unwrap();
                            let obj_meta = table.get(prev_path).unwrap().unwrap().value();
                            let start_pos = match obj_meta.obj.materialized_pos {
                                ObjLoc::Pack { start_pos } => start_pos,
                                ObjLoc::Interchange => return Ok(None),
                            };
                            start_pos + obj_meta.obj.materialized_size
                        } else {
                            0
                        });
                        continue;
                    }
                    if a.is_some()
                        && let Some(left_empty) = left_empty
                    {
                        return Ok(Some(NextReloc::Reloc(Relocation {
                            gap: ShardIterGap {
                                left_empty,
                                right_some: ShardIterIndex(i),
                            },
                            dst_pack_pos: dst_pack_pos.unwrap(),
                        })));
                    }
                }
                Ok(left_empty
                    .map(|left_empty| NextReloc::Truncate(TruncateIterIndex { left_empty })))
            })
            .await
    }
    pub async fn clear_interchange(
        &mut self,
        shard_id: ShardId,
        reloc: Relocation,
    ) -> anyhow::Result<()> {
        self.db
            .write(move |write| {
                let old_iter_key = ShardIterKey {
                    shard: shard_id,
                    index: reloc.gap.right_some,
                };
                let new_iter_key = ShardIterKey {
                    shard: shard_id,
                    index: reloc.gap.left_empty,
                };
                let mut table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
                if table.get(new_iter_key)?.is_some() {
                    return Ok(((), CloseWrite::Rollback));
                }
                let obj_path = {
                    let Some(obj_path) = table.remove(old_iter_key)? else {
                        return Ok(((), CloseWrite::Rollback));
                    };
                    obj_path.value()
                };
                table.insert(new_iter_key, obj_path.clone())?;
                let mut table = write_table_result(write.open_table(OBJ_META_TABLE))?;
                let mut obj_meta = table.get(&obj_path)?.unwrap().value();
                obj_meta.key = new_iter_key;
                obj_meta.obj.materialized_pos = ObjLoc::Pack {
                    start_pos: reloc.dst_pack_pos,
                };
                table.insert(obj_path, obj_meta)?;
                let mut table = write_table_result(write.open_table(SHARD_META_TABLE))?;
                let mut shard_meta = table.get(shard_id)?.unwrap().value();
                shard_meta.interchange_stage = None;
                table.insert(shard_id, shard_meta)?;
                Ok(((), CloseWrite::Commit))
            })
            .await
    }
    pub async fn set_pack_to_interchange(
        &mut self,
        shard_id: ShardId,
        reloc: Relocation,
    ) -> anyhow::Result<()> {
        self.set_interchange_stage(shard_id, InterchangeStage::PackToInterchange(reloc))
            .await
    }
    pub async fn set_interchange_to_pack(
        &mut self,
        shard_id: ShardId,
        reloc: Relocation,
    ) -> anyhow::Result<()> {
        self.set_interchange_stage(shard_id, InterchangeStage::InterchangeToPack(reloc))
            .await
    }
    async fn set_interchange_stage(
        &mut self,
        shard_id: ShardId,
        interchange: InterchangeStage,
    ) -> anyhow::Result<()> {
        self.db
            .write(move |write| {
                let reloc = match &interchange {
                    InterchangeStage::PackToInterchange(relocation) => relocation,
                    InterchangeStage::InterchangeToPack(relocation) => relocation,
                };
                let table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
                let left_really_empty = table
                    .get(ShardIterKey {
                        shard: shard_id,
                        index: reloc.gap.left_empty,
                    })?
                    .is_none();
                if !left_really_empty {
                    return Ok(((), CloseWrite::Rollback));
                }
                let Some(obj_path) = table.get(ShardIterKey {
                    shard: shard_id,
                    index: reloc.gap.right_some,
                })?
                else {
                    return Ok(((), CloseWrite::Rollback));
                };
                let obj_path = obj_path.value();
                let mut table = write_table_result(write.open_table(OBJ_META_TABLE))?;
                let mut obj_meta = table.get(obj_path.clone())?.unwrap().value();
                match obj_meta.obj.materialized_pos {
                    ObjLoc::Pack { start_pos: _ } => (),
                    ObjLoc::Interchange => return Ok(((), CloseWrite::Rollback)),
                }
                if matches!(&interchange, InterchangeStage::InterchangeToPack(_)) {
                    obj_meta.obj.materialized_pos = ObjLoc::Interchange;
                    table.insert(obj_path, obj_meta)?.unwrap();
                }
                let mut table = write_table_result(write.open_table(SHARD_META_TABLE))?;
                let mut shard_meta = table.get(&shard_id)?.unwrap().value();
                shard_meta.interchange_stage = Some(interchange);
                table.insert(shard_id, shard_meta)?;
                Ok(((), CloseWrite::Commit))
            })
            .await
    }
    pub async fn truncate(
        &mut self,
        shard_id: ShardId,
        truncate: TruncateIterIndex,
    ) -> anyhow::Result<Option<TruncatePackFile>> {
        self.db
            .write(move |write| {
                let table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
                if table
                    .get(&ShardIterKey {
                        shard: shard_id,
                        index: truncate.left_empty,
                    })?
                    .is_some()
                {
                    return Ok((None, CloseWrite::Rollback));
                };
                let prev_some_iter_index = truncate.left_empty.0.checked_sub(1).map(ShardIterIndex);
                let truncate_pack_pos = match prev_some_iter_index {
                    Some(iter_index) => {
                        let Some(obj_path) = table.get(&ShardIterKey {
                            shard: shard_id,
                            index: iter_index,
                        })?
                        else {
                            return Ok((None, CloseWrite::Rollback));
                        };
                        let table = write_table_result(write.open_table(OBJ_META_TABLE))?;
                        let obj_meta = table.get(obj_path.value())?.unwrap().value();
                        match obj_meta.obj.materialized_pos {
                            ObjLoc::Pack { start_pos } => {
                                start_pos + obj_meta.obj.materialized_size
                            }
                            ObjLoc::Interchange => return Ok((None, CloseWrite::Rollback)),
                        }
                    }
                    None => 0,
                };
                let mut table = write_table_result(write.open_table(SHARD_META_TABLE))?;
                let mut shard_meta = {
                    let Some(shard_meta) = table.get(shard_id)? else {
                        return Ok((None, CloseWrite::Rollback));
                    };
                    shard_meta.value()
                };
                shard_meta.next_iter_index = truncate.left_empty;
                shard_meta.write_pos = truncate_pack_pos;
                table.insert(shard_id, shard_meta)?;
                Ok((
                    Some(TruncatePackFile {
                        truncate_pos: truncate_pack_pos,
                    }),
                    CloseWrite::Commit,
                ))
            })
            .await
    }
}

#[derive(Debug, Clone, Error)]
pub enum RenameError {
    #[error("source not exists")]
    SourceNotExist,
    #[error("blocked by {by:?}")]
    Blocked { by: CanPathBuf },
}
#[derive(Debug, Clone, Error)]
pub enum InsertError {
    #[error("blocked by {by:?}")]
    Blocked { by: CanPathBuf },
}

#[derive(Debug, Clone)]
pub struct TruncatePackFile {
    pub truncate_pos: usize,
}
#[derive(Debug, Clone)]
pub enum NextReloc {
    Truncate(TruncateIterIndex),
    Reloc(Relocation),
}
#[derive(Debug, Clone)]
pub struct TruncateIterIndex {
    pub left_empty: ShardIterIndex,
}

const SHARD_META_TABLE: TableDefinition<Bincode<ShardId>, Bincode<ShardMeta>> =
    TableDefinition::new("shard_meta");
const SHARD_ITER_TABLE: TableDefinition<Bincode<ShardIterKey>, Bincode<CanPathBuf>> =
    TableDefinition::new("shard_iter");
const OBJ_META_TABLE: TableDefinition<Bincode<CanPathBuf>, Bincode<ObjIndexMeta>> =
    TableDefinition::new("obj_meta");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct ShardId(pub usize);
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardMeta {
    pub write_pos: usize,
    pub interchange_stage: Option<InterchangeStage>,
    pub next_iter_index: ShardIterIndex,
    pub crypt_key: SymKey,
}
fn new_shard_meta() -> ShardMeta {
    let random_key: [u8; tokio_chacha20::KEY_BYTES] = rand::random();
    ShardMeta {
        write_pos: 0,
        interchange_stage: None,
        next_iter_index: ShardIterIndex(0),
        crypt_key: SymKey(Box::new(random_key)),
    }
}
#[derive(Debug, Clone, Encode, Decode)]
pub enum InterchangeStage {
    PackToInterchange(Relocation),
    InterchangeToPack(Relocation),
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct Relocation {
    pub gap: ShardIterGap,
    pub dst_pack_pos: usize,
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardIterGap {
    pub left_empty: ShardIterIndex,
    pub right_some: ShardIterIndex,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SymKey(pub Box<[u8; tokio_chacha20::KEY_BYTES]>);

#[derive(Debug, Clone, Encode, Decode)]
pub struct ObjIndexMeta {
    pub key: ShardIterKey,
    pub obj: ObjMeta,
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct ObjMeta {
    pub materialized_pos: ObjLoc,
    pub materialized_size: usize,
    pub compressed: bool,
    pub hash: ObjHash,
}
#[derive(Debug, Clone, Encode, Decode)]
pub enum ObjLoc {
    Pack { start_pos: usize },
    Interchange,
}

#[derive(Debug, Clone)]
pub struct NewObjMeta {
    pub materialized_start_pos: usize,
    pub materialized_size: usize,
    pub compressed: bool,
    pub hash: ObjHash,
}
fn convert_obj_meta(new: NewObjMeta) -> ObjMeta {
    ObjMeta {
        materialized_pos: ObjLoc::Pack {
            start_pos: new.materialized_start_pos,
        },
        materialized_size: new.materialized_size,
        compressed: new.compressed,
        hash: new.hash,
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ObjHash(pub Box<[u8; 32]>);

#[derive(Debug, Clone)]
pub enum ObjMetaKey {
    IterKey(ShardIterKey),
    Path(CanPathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct ShardIterKey {
    pub shard: ShardId,
    pub index: ShardIterIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct ShardIterIndex(pub usize);

fn blocking_remove_obj(
    write: &redb::WriteTransaction,
    obj_path: CanPathBuf,
) -> anyhow::Result<Option<ObjIndexMeta>> {
    let mut table = write_table_result(write.open_table(OBJ_META_TABLE))?;
    let Some(obj_meta) = table.remove(&obj_path)? else {
        return Ok(None);
    };
    let obj_meta = obj_meta.value().clone();
    let mut table = write_table_result(write.open_table(SHARD_ITER_TABLE))?;
    let read_obj_path = table.remove(&obj_meta.key)?.unwrap();
    assert_eq!(read_obj_path.value(), obj_path);
    Ok(Some(obj_meta))
}
