use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use bincode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub struct CanPathBuf(PathBuf);
impl CanPathBuf {
    pub fn as_path(&self) -> &PathBuf {
        &self.0
    }
    pub fn to_path_buf(self) -> PathBuf {
        self.0
    }
    pub fn parent(&self) -> Option<Self> {
        Some(Self(self.0.parent()?.to_path_buf()))
    }
    pub fn file_name(&self) -> Option<&OsStr> {
        self.0.file_name()
    }
    pub fn join_seg(&self, seg: impl AsRef<Path>) -> Option<Self> {
        let seg = seg.as_ref();
        match classify_seg(seg) {
            SegClass::Child => {
                let a = self.0.join(seg);
                Some(Self(a))
            }
            SegClass::Root => None,
            SegClass::Invalid => None,
            SegClass::Current => Some(self.clone()),
            SegClass::Parent => {
                let mut a = self.0.clone();
                if !a.pop() {
                    return None;
                };
                Some(Self(a))
            }
        }
    }
    pub fn subtract(&self, other: &Self) -> Option<PathBuf> {
        if self.0.iter().count() < other.0.iter().count() {
            return None;
        }
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            if a != b {
                return None;
            }
        }
        let n = self.0.iter().zip(other.0.iter()).count();
        let mut left = PathBuf::new();
        for seg in self.0.iter().skip(n) {
            left.push(seg);
        }
        Some(left)
    }
}
pub fn canonicalize_path(path: impl AsRef<Path>) -> Option<CanPathBuf> {
    let path = path.as_ref();
    let mut canonical = vec![];
    for seg in path.iter() {
        match classify_seg(seg) {
            SegClass::Child => canonical.push(seg),
            SegClass::Invalid => panic!(),
            SegClass::Root => (),
            SegClass::Current => (),
            SegClass::Parent => {
                canonical.pop()?;
            }
        }
    }
    let mut path = Path::new("/").to_path_buf();
    for seg in canonical {
        path = path.join(seg);
    }
    Some(CanPathBuf(path))
}
#[cfg(test)]
#[test]
fn test_canonicalize_path() {
    let path = "/";
    assert_eq!(canonicalize_path(path).unwrap().as_path(), Path::new("/"));
    let path = "/a";
    assert_eq!(canonicalize_path(path).unwrap().as_path(), Path::new("/a"));
    let path = "./a";
    assert_eq!(canonicalize_path(path).unwrap().as_path(), Path::new("/a"));
    let path = "a";
    assert_eq!(canonicalize_path(path).unwrap().as_path(), Path::new("/a"));
    let path = "a/../b";
    assert_eq!(canonicalize_path(path).unwrap().as_path(), Path::new("/b"));
}

fn classify_seg(seg: impl AsRef<Path>) -> SegClass {
    let seg = seg.as_ref();
    match seg.as_os_str().as_bytes() {
        b"." => return SegClass::Current,
        b".." => return SegClass::Parent,
        b"/" => return SegClass::Root,
        _ => (),
    };
    if seg.as_os_str().as_bytes().contains(&b'/') {
        return SegClass::Invalid;
    }
    SegClass::Child
}
enum SegClass {
    Child,
    Invalid,
    Current,
    Parent,
    Root,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtract() {
        let a = "/a/b/c";
        let b = "/a/b";
        let a = canonicalize_path(a).unwrap();
        let b = canonicalize_path(b).unwrap();
        let c = a.subtract(&b).unwrap();
        assert_eq!(c, Path::new("c"));
        assert!(b.subtract(&a).is_none());

        let a = "/c/d";
        let b = "/a/";
        let a = canonicalize_path(a).unwrap();
        let b = canonicalize_path(b).unwrap();
        assert!(a.subtract(&b).is_none());
        assert!(b.subtract(&a).is_none());
    }
}
