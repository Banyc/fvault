use std::collections::VecDeque;

use anyhow::Context;
use bincode::{Decode, Encode};
use redb::{ReadableTable, TableDefinition};

use crate::{
    db_val::Bincode,
    index::{read_table_result, write_table_result},
    path::CanPathBuf,
};

const TREE_TABLE: TableDefinition<Bincode<CanPathBuf>, Bincode<Node>> =
    TableDefinition::new("tree");
#[derive(Debug, Clone, Encode, Decode)]
pub enum Node {
    NonLeaf(NonLeafNode),
    Leaf(LeafNode),
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct NonLeafNode {
    pub children: Vec<String>,
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct LeafNode {}

pub fn blocking_list(
    read: &redb::ReadTransaction,
    path: CanPathBuf,
) -> anyhow::Result<Option<Node>> {
    let Some(table) = read_table_result(read.open_table(TREE_TABLE))? else {
        return Ok(None);
    };
    let Some(node) = table.get(path)? else {
        return Ok(None);
    };
    Ok(Some(node.value()))
}

pub fn blocking_visit(
    read: &redb::ReadTransaction,
    path: CanPathBuf,
) -> anyhow::Result<Vec<(CanPathBuf, Node)>> {
    let Some(table) = read_table_result(read.open_table(TREE_TABLE))? else {
        return Ok(vec![]);
    };
    bfs(path, |path| Ok(table.get(path)?.map(|x| x.value())))
}

pub fn blocking_remove(
    write: &redb::WriteTransaction,
    path: CanPathBuf,
) -> anyhow::Result<Vec<(CanPathBuf, Node)>> {
    let mut table = write_table_result(write.open_table(TREE_TABLE))?;
    if table.get(&path)?.is_none() {
        return Ok(vec![]);
    };
    let mut removed_nodes = bfs(path.clone(), |path| {
        Ok(table.remove(path)?.map(|x| x.value()))
    })?;
    let mut prev_path = path;
    while let Some(parent_path) = prev_path.parent() {
        let name = prev_path.file_name().unwrap().to_str().context("OsStr")?;
        let mut parent = {
            let parent = table.remove(&parent_path)?.unwrap().value();
            removed_nodes.push((parent_path.clone(), parent.clone()));
            match parent {
                Node::NonLeaf(non_leaf_node) => non_leaf_node,
                Node::Leaf(_) => panic!(),
            }
        };
        if parent.children.len() != 1 {
            let i = parent
                .children
                .iter()
                .position(|child| *child == name)
                .unwrap();
            parent.children.swap_remove(i);
            table.insert(parent_path.clone(), Node::NonLeaf(parent))?;
            break;
        }
        prev_path = parent_path;
    }
    Ok(removed_nodes)
}
pub fn blocking_insert(
    write: &redb::WriteTransaction,
    path: CanPathBuf,
) -> anyhow::Result<Result<(), InsertError>> {
    let mut table = write_table_result(write.open_table(TREE_TABLE))?;
    if let Some(_node) = table.insert(path.clone(), Node::Leaf(LeafNode {}))? {
        return Ok(Err(InsertError::Blocked { by: path }));
    };
    let mut prev_path = path;
    while let Some(parent_path) = prev_path.parent() {
        dbg!(&prev_path);
        let name = prev_path.file_name().unwrap().to_str().context("OsStr")?;
        let parent = table.remove(&parent_path)?.map(|x| x.value());
        let parent = match parent {
            Some(parent) => {
                let mut parent = match parent {
                    Node::NonLeaf(non_leaf_node) => non_leaf_node,
                    Node::Leaf(_) => return Ok(Err(InsertError::Blocked { by: parent_path })),
                };
                if !parent.children.iter().any(|x| x == name) {
                    parent.children.push(name.to_string());
                }
                parent
            }
            None => NonLeafNode {
                children: vec![name.to_string()],
            },
        };
        table.insert(parent_path.clone(), Node::NonLeaf(parent))?;
        prev_path = parent_path;
    }
    Ok(Ok(()))
}
#[derive(Debug, Clone)]
pub enum InsertError {
    Blocked { by: CanPathBuf },
}

fn bfs(
    path: CanPathBuf,
    mut get_node: impl FnMut(&CanPathBuf) -> anyhow::Result<Option<Node>>,
) -> anyhow::Result<Vec<(CanPathBuf, Node)>> {
    let mut visited_nodes = vec![];
    let mut visit_queue = VecDeque::from_iter([path.clone()]);
    while let Some(path) = visit_queue.pop_front() {
        let Some(node) = get_node(&path)? else {
            continue;
        };
        visited_nodes.push((path.clone(), node.clone()));
        match node {
            Node::NonLeaf(non_leaf_node) => {
                for child in &non_leaf_node.children {
                    visit_queue.push_back(path.join_seg(child).unwrap());
                }
            }
            Node::Leaf(_) => (),
        }
    }
    Ok(visited_nodes)
}
