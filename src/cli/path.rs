use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};

use tokio::fs::{canonicalize, metadata, read_dir, try_exists};

pub const REPO_DIR_NAME: &str = ".fvault";
const WALK_IGNORE_DOT_FILES: bool = true;

pub async fn repo_rel_path(path: impl AsRef<Path>) -> anyhow::Result<Option<RepoRelPath>> {
    let path = canonicalize(path.as_ref()).await?;
    let mut search_repo_base: &Path = &path;
    let mut rel_path = "".into();
    loop {
        let repo_dir = search_repo_base.join(REPO_DIR_NAME);
        if try_exists(&repo_dir).await? {
            return Ok(Some(RepoRelPath {
                repo_base: search_repo_base.to_path_buf(),
                rel_path,
            }));
        }
        let Some(parent) = search_repo_base.parent() else {
            break;
        };
        rel_path = Path::new(search_repo_base.file_name().unwrap()).join(rel_path);
        search_repo_base = parent;
    }
    Ok(None)
}
#[derive(Debug, Clone)]
pub struct RepoRelPath {
    pub repo_base: PathBuf,
    pub rel_path: PathBuf,
}

pub async fn visit_files(path: impl AsRef<Path>) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths_to_visit = VecDeque::from_iter([path.as_ref().to_path_buf()]);
    let mut visited_files = vec![];
    while let Some(path) = paths_to_visit.pop_front() {
        let meta_data = metadata(&path).await?;
        if meta_data.is_file() {
            visited_files.push(path);
            continue;
        }
        if !meta_data.is_dir() {
            continue;
        }
        if WALK_IGNORE_DOT_FILES
            && let Some(file_name) = path.file_name()
            && file_name.to_string_lossy().starts_with(".")
        {
            continue;
        }
        let mut read_dir = read_dir(&path).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            paths_to_visit.push_back(entry.path());
        }
    }
    Ok(visited_files)
}
