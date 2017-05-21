#![recursion_limit = "1024"]

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate error_chain;

mod errors {
    error_chain!{}
}
use errors::*;

extern crate clap;
extern crate time;
extern crate fuse;
extern crate libc;

mod hfs;

fn main() {

    if let Err(ref e) = run() {
        use ::std::io::Write;
        let stderr = &mut ::std::io::stderr();
        let errmsg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(errmsg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(errmsg);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(errmsg);
        }

        ::std::process::exit(1);
    }

}

fn run() -> Result<()> {

    env_logger::init().unwrap();
    trace!("Starting");

    use clap::{Arg, App};

    let app = App::new("S3 Hierarchical Filesystem")
        .version("0.1.0")
        .author("Alister Lee <dev@shortepic.com>")
        .about("AWS S3-backed infinitely-expandable mountable filesystem.")
        .arg(Arg::with_name("MOUNTPATH")
            .default_value("/tmp/fs")
            // .required(true)
            .help("path to mount the filesystem at"))
        .arg(Arg::with_name("BACKINGPATH")
            .default_value("/tmp/back")
            // .required(true)
            .help("path where underlying files will be "));

    let cmdline = app.get_matches();
    let mountpath = cmdline.value_of("MOUNTPATH").unwrap();
    let backingpath = cmdline.value_of("BACKINGPATH").unwrap();

    trace!("{:?}", cmdline);

    match cmdline.subcommand_name() {
        None => hfs::S3HierarchicalFilesystem::mount(mountpath, backingpath),
        _ => bail!("incorrect options"),
    }
}
