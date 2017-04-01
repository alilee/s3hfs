use super::errors::*;

use fuse;
use fuse::{Filesystem, Request, ReplyAttr, FileAttr};

use libc::ENOSYS;

use time::Timespec;
use std;

pub struct S3HierarchicalFilesystem<'a> {
    mountpath: &'a str,
    backingpath: &'a str,
}

impl<'a> S3HierarchicalFilesystem<'a> {
    pub fn mount(mp: &str, bp: &str) -> Result<()> {
        let fs = S3HierarchicalFilesystem {
            mountpath: mp,
            backingpath: bp,
        };
        fuse::mount(fs, &mp, &[]).chain_err(|| "mounting filesystem")
    }
}

fn fuse_type(ft: &std::fs::FileType) -> Result<fuse::FileType> {
    if ft.is_file() {
        return Ok(fuse::FileType::RegularFile);
    }
    if ft.is_dir() {
        return Ok(fuse::FileType::Directory);
    }
    if ft.is_symlink() {
        return Ok(fuse::FileType::Symlink);
    }
    bail!("unknown filetype")
}

fn timespec(st: &std::time::SystemTime) -> Timespec {
    if let Ok(dur_since_epoch) = st.duration_since(std::time::UNIX_EPOCH) {
        Timespec::new(dur_since_epoch.as_secs() as i64,
                      dur_since_epoch.subsec_nanos() as i32)
    } else {
        Timespec::new(0, 0)
    }
}

impl<'a> Filesystem for S3HierarchicalFilesystem<'a> {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        use std::time::SystemTime;

        println!("getattr(ino={})", ino);

        if let Ok(m) = std::fs::metadata(self.backingpath) {
            trace!("{:?}", m);
            let kind = fuse_type(&m.file_type()).unwrap();
            let mode = m.permissions().mode();
            let attr = FileAttr {
                ino: m.ino(),
                size: m.len(),
                blocks: m.blocks(),
                atime: timespec(&m.accessed().unwrap_or(SystemTime::now())),
                mtime: timespec(&m.modified().unwrap_or(SystemTime::now())),
                ctime: Timespec::new(m.ctime(), m.ctime_nsec() as i32),
                crtime: timespec(&m.created().unwrap_or(SystemTime::now())),
                kind: kind,
                perm: mode as u16,
                nlink: m.nlink() as u32,
                uid: m.uid(),
                gid: m.gid(),
                rdev: m.rdev() as u32,
                flags: 0,
            };
            trace!("{:?}", attr);
        }

        reply.error(ENOSYS);
    }
}
