use super::errors::*;

use fuse;
use fuse::{Filesystem, Request, ReplyAttr, ReplyDirectory, ReplyEntry, FileAttr, ReplyOpen,
           ReplyData, ReplyEmpty, ReplyCreate, ReplyWrite};

use libc::{ENOSYS, ENOENT};

use time::Timespec;
use std;
use std::ffi::OsStr;
use std::fs;
use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::collections::HashMap;

pub struct S3HierarchicalFilesystem<'a> {
    _mount_path: &'a str,
    _backing_path: &'a str,
    ino_paths: HashMap<u64, PathBuf>,
    files: HashMap<u64, File>,
}

impl<'a> S3HierarchicalFilesystem<'a> {
    pub fn mount(mp: &str, bp: &str) -> Result<()> {
        let mut fs = S3HierarchicalFilesystem {
            _mount_path: mp,
            _backing_path: bp,
            ino_paths: HashMap::new(),
            files: HashMap::new(),
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

macro_rules! ino_path_or_return {
    ($self:ident, $parent:expr, $reply:ident) => ({
        let path = match $self.ino_paths.get($parent) {
            Some(p) => p,
            None => {
                error!("parent not found in cache");
                $reply.error(ENOSYS);
                return;
            }
        };
        path.clone()
    })
}

macro_rules! full_path_or_return {
    ($self:ident, $parent:expr, $name:expr, $reply:ident) => ({
        let parent_path = match $self.ino_paths.get($parent) {
            Some(p) => p,
            None => {
                error!("parent not found in cache");
                $reply.error(ENOSYS);
                return;
            }
        };
        let new_name = match $name.to_str() {
            Some(p) => p,
            None => {
                error!("unable to read name");
                $reply.error(ENOSYS);
                return;
            }
        };
        Path::new(&parent_path).join(new_name).clone()
    })
}

macro_rules! file_handle_or_return {
    ($self:ident, $fh:expr, $reply:ident) => (
        match $self.files.get($fh) {
            Some(f) => f,
            None => {
                error!("File handle not found: {}", $fh);
                $reply.error(ENOENT);
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
        trace!("getattr(ino={})", ino);

        let path = ino_path_or_return!(self, &ino, reply);
        let metadata = ok_or_return_error!(std::fs::metadata(path), ENOENT, reply);

        debug!("{:?}", metadata);
        let attr = fileattr_from(&metadata);
        let ttl = Timespec::new(1, 0);
        reply.attr(&ttl, &attr);
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup(parent={}, name={:?})", parent, name);

        let path = full_path_or_return!(self, &parent, name, reply);

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

        trace!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);

        let path: PathBuf = ino_path_or_return!(self, &ino, reply);

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
            debug!("Adding: {}, {}, {:?}, \"{}\"",
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

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        trace!("open(ino={}, flags={})", ino, _flags);

        let path = ino_path_or_return!(self, &ino, reply);

        match File::open(path) {
            Ok(f) => {
                // FIXME: race condition
                let fh: u64 = *self.files.keys().max().unwrap_or(&10u64) + 1;
                self.files.insert(fh, f);
                trace!("opened file handle: {}", fh);
                reply.opened(fh, 0);
            }
            Err(e) => {
                error!("File::open: {:?}", e);
                reply.error(e.raw_os_error().unwrap_or(ENOENT));
            }
        };
    }

    fn read(&mut self,
            _req: &Request,
            _ino: u64,
            fh: u64,
            offset: u64,
            size: u32,
            reply: ReplyData) {

        use std::io::Read;

        trace!("read(ino={}, fh={}, offset={}, size={})",
               _ino,
               fh,
               offset,
               size);

        let mut f = file_handle_or_return!(self, &fh, reply);
        debug!("File: {:?}", f);

        let mut buffer = Vec::with_capacity(size as usize);
        buffer.resize(size as usize, 0u8);

        match f.read_exact(&mut buffer[..]) {
            Ok(_) => {
                debug!("Read {} bytes", size);
                reply.data(&buffer);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(ENOENT)),
        }
    }

    fn release(&mut self,
               _req: &Request,
               _ino: u64,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool,
               reply: ReplyEmpty) {
        trace!("release(ino={}, fh={}, flags={}, lock_owner={}, flush={})",
               _ino,
               fh,
               _flags,
               _lock_owner,
               _flush);

        match self.files.remove(&fh) {
            Some(_) => {
                debug!("closed file handle: {}", fh);
                reply.ok();
            }
            None => {
                error!("File handle not found: {}", fh);
                reply.error(ENOENT);
            }
        };
    }

    fn create(&mut self,
              _req: &Request,
              parent: u64,
              name: &OsStr,
              _mode: u32,
              _flags: u32,
              reply: ReplyCreate) {
        trace!("create(parent={}, name={:?}, mode={}, flags={})",
               parent,
               name,
               _mode,
               _flags);

        let path = full_path_or_return!(self, &parent, name, reply);

        match File::create(&path) {
            Ok(f) => {
                trace!("File created: {:?}", f);
                let fh: u64 = *self.files.keys().max().unwrap_or(&10u64) + 1;
                debug!("Handle: {}", fh);
                self.files.insert(fh, f);
                match path.metadata() {
                    Ok(metadata) => {
                        let attr = fileattr_from(&metadata);
                        self.ino_paths.insert(attr.ino, path);
                        let ttl = Timespec::new(1, 0);
                        reply.created(&ttl, &attr, 0, fh, 0);
                    }
                    Err(e) => {
                        error!("std::fs::metadata: {}", e);
                        reply.error(e.raw_os_error().unwrap_or(ENOENT));
                    }
                }
            }
            Err(e) => {
                error!("File::create: {:?}", e);
                reply.error(e.raw_os_error().unwrap_or(ENOENT));
            }
        }
    }

    fn write(&mut self,
             _req: &Request,
             _ino: u64,
             fh: u64,
             offset: u64,
             data: &[u8],
             _flags: u32,
             reply: ReplyWrite) {

        use std::io::{Seek, SeekFrom, Write};

        trace!("write(ino={}, fh={}, offset={}, data={:?}, flags={})",
               _ino,
               fh,
               offset,
               data,
               _flags);

        let mut f = file_handle_or_return!(self, &fh, reply);
        debug!("File: {:?}", f);

        if let Err(e) = f.seek(SeekFrom::Start(offset)) {
            error!("Unable to seek to position: {}", offset);
            reply.error(e.raw_os_error().unwrap_or(ENOENT));
            return;
        }

        match f.write(&data) {
            Ok(size) => {
                debug!("Written {} bytes", size);
                reply.written(size as u32);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(ENOENT)),
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        trace!("mkdir(parent={}, name={:?}, mode={})", parent, name, _mode);

        let path = full_path_or_return!(self, &parent, name, reply);

        match fs::create_dir(&path) {
            Ok(()) => {
                trace!("Subdir created");
                match path.metadata() {
                    Ok(metadata) => {
                        let attr = fileattr_from(&metadata);
                        self.ino_paths.insert(attr.ino, path);
                        let ttl = Timespec::new(1, 0);
                        reply.entry(&ttl, &attr, 0);
                    }
                    Err(e) => {
                        error!("std::fs::metadata: {}", e);
                        reply.error(e.raw_os_error().unwrap_or(ENOENT));
                    }
                }
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(ENOENT)),
        }
    }
}
