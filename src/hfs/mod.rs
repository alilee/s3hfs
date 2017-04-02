use super::errors::*;

use fuse;
use fuse::{Filesystem, Request, ReplyAttr, ReplyDirectory, FileAttr};

use libc::{ENOSYS, ENOENT};

use time::Timespec;
use std;
use std::fs;
use std::time::SystemTime;

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

fn filetype_tryfrom(ft: &fs::FileType) -> Result<fuse::FileType> {
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

fn timespec_from(st: &SystemTime) -> Timespec {
    if let Ok(dur_since_epoch) = st.duration_since(std::time::UNIX_EPOCH) {
        Timespec::new(dur_since_epoch.as_secs() as i64,
                      dur_since_epoch.subsec_nanos() as i32)
    } else {
        Timespec::new(0, 0)
    }
}

fn fileattr_from(m: &std::fs::Metadata) -> FileAttr {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::time::SystemTime;

    let kind = filetype_tryfrom(&m.file_type()).unwrap();
    let mode = m.permissions().mode();
    FileAttr {
        ino: m.ino(),
        size: m.len(),
        blocks: m.blocks(),
        atime: timespec_from(&m.accessed().unwrap_or(SystemTime::now())),
        mtime: timespec_from(&m.modified().unwrap_or(SystemTime::now())),
        ctime: Timespec::new(m.ctime(), m.ctime_nsec() as i32),
        crtime: timespec_from(&m.created().unwrap_or(SystemTime::now())),
        kind: kind,
        perm: mode as u16,
        nlink: m.nlink() as u32,
        uid: m.uid(),
        gid: m.gid(),
        rdev: m.rdev() as u32,
        flags: 0,
    }
}

impl<'a> Filesystem for S3HierarchicalFilesystem<'a> {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr(ino={})", ino);
        match std::fs::metadata(self.backingpath) {
            Ok(metadata) => {
                debug!("{:?}", metadata);
                let attr = fileattr_from(&metadata);
                if ino == 1 {
                    let ttl = Timespec::new(1, 0);
                    reply.attr(&ttl, &attr);
                    debug!("{:?}", attr);
                } else {
                    reply.error(ENOSYS);
                }
            }
            Err(_) => {
                reply.error(ENOSYS);
            }
        };
    }

    fn readdir(&mut self,
               _req: &Request,
               ino: u64,
               fh: u64,
               offset: u64,
               mut reply: ReplyDirectory) {
        use std::os::unix::fs::DirEntryExt;

        info!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        trace!("{:?}", _req);

        if ino == 1 {
            if offset == 0 {
                reply.add(1, 0, fuse::FileType::Directory, ".");
                reply.add(1, 1, fuse::FileType::Directory, "..");
            }
            for (i, entry) in std::fs::read_dir(self.backingpath).unwrap().enumerate() {
                if (i as u64) < offset {
                    continue;
                };
                let entry = entry.unwrap();
                if reply.add(entry.ino(),
                             (i + 2) as u64,
                             filetype_tryfrom(&entry.file_type().unwrap()).unwrap(),
                             entry.file_name()) {
                    break;
                };
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }
}
