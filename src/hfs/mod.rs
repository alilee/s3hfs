use super::errors::*;

use fuse;
use fuse::{Filesystem, Request, ReplyAttr, ReplyDirectory, ReplyEntry, FileAttr};

use libc::{ENOSYS, ENOENT};

use time::Timespec;
use std;
use std::ffi::OsStr;
use std::fs;
use std::time::SystemTime;
use std::path::{Path, PathBuf};

pub struct S3HierarchicalFilesystem<'a> {
    mount_path: &'a str,
    backing_path: &'a str,
    ino_paths: std::collections::HashMap<u64, PathBuf>,
}

impl<'a> S3HierarchicalFilesystem<'a> {
    pub fn mount(mp: &str, bp: &str) -> Result<()> {
        let mut fs = S3HierarchicalFilesystem {
            mount_path: mp,
            backing_path: bp,
            ino_paths: std::collections::HashMap::new(),
        };
        fs.ino_paths.insert(1, PathBuf::from(bp));
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

fn dir_from(entry_opt: std::io::Result<fs::DirEntry>) -> Option<(u64, fuse::FileType, String)> {
    use std::os::unix::fs::DirEntryExt;

    let mut result = None;
    if let Ok(entry) = entry_opt {
        if let Ok(fsfiletype) = entry.file_type() {
            if let Ok(filetype) = filetype_tryfrom(&fsfiletype) {
                if let Ok(filename) = entry.file_name().into_string() {
                    result = Some((entry.ino(), filetype, filename));
                }
            }
        }
    }
    result
}

macro_rules! unwrap_or_return_error {
    ($v:expr, $log:expr, $err:expr, $reply:ident) => (
        match $v {
            Some(v) => v,
            None => {
                error!($log);
                $reply.error($err);
                return;
            }
        }
    )
}

macro_rules! ok_or_return_error {
    ($v:expr, $err:expr, $reply:ident) => (
        match $v {
            Ok(v) => v,
            Err(e) => {
                error!("{:?}", e);
                $reply.error($err);
                return;
            }
        }
    )
}

impl<'a> Filesystem for S3HierarchicalFilesystem<'a> {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr(ino={})", ino);
        debug!("req: {:?}", _req);

        let path = unwrap_or_return_error!(self.ino_paths.get(&ino),
                                           "ino not found in cache",
                                           ENOENT,
                                           reply);
        let metadata = ok_or_return_error!(std::fs::metadata(path), ENOENT, reply);

        debug!("{:?}", metadata);
        let attr = fileattr_from(&metadata);
        let ttl = Timespec::new(1, 0);
        reply.attr(&ttl, &attr);
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup(parent={}, name={:?})", parent, name);

        let parent_path = unwrap_or_return_error!(self.ino_paths.get(&parent),
                                                  "ino not found in cache",
                                                  ENOSYS,
                                                  reply);

        let file_name =
            unwrap_or_return_error!(name.to_str(), "unable to read name", ENOSYS, reply);

        let path = Path::new(&parent_path).join(file_name);
        debug!("Path: {:?}", path);
        match path.metadata() {
            Ok(metadata) => {
                debug!("{:?}", metadata);
                let attr = fileattr_from(&metadata);
                let ttl = Timespec::new(1, 0);
                debug!("warning: generation assumed 0");
                reply.entry(&ttl, &attr, 0);
                debug!("{:?}", attr);
            }
            Err(e) => {
                info!("std::fs::metadata: {}", e);
                reply.error(ENOENT);
            }
        };
    }

    fn readdir(&mut self,
               _req: &Request,
               ino: u64,
               fh: u64,
               offset: u64,
               mut reply: ReplyDirectory) {

        info!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        trace!("{:?}", _req);

        let path = {
            let x = unwrap_or_return_error!(self.ino_paths.get(&ino),
                                            "ino not found in cache",
                                            ENOSYS,
                                            reply);
            x.clone()
        };

        if offset < 1 {
            reply.add(ino, 0, fuse::FileType::Directory, ".");
            reply.add(ino, 1, fuse::FileType::Directory, "..");
        };

        let rd = ok_or_return_error!(path.read_dir(), ENOENT, reply);

        for (i, entry_opt) in rd.enumerate() {
            let entry_offset = (i + 2) as u64;
            if offset >= entry_offset {
                continue;
            };
            let (ino, filetype, filename) = match dir_from(entry_opt) {
                Some(x) => x,
                None => {
                    error!("unable to extract directory entry");
                    reply.error(ENOENT);
                    return;
                }
            };
            debug!("Adding: {}, {}, {:?}, {}",
                   ino,
                   entry_offset,
                   filetype,
                   filename);
            if reply.add(ino, entry_offset, filetype, &filename) {
                break;
            }
            self.ino_paths.insert(ino, path.join(filename));
        }
        reply.ok();
    }
}
