#![feature(rand)]
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
extern crate rand;

use rand::{StdRng, SeedableRng};

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

    let app = App::new("Pasthrough Filesystem Hammer")
        .version("0.1.0")
        .author("Alister Lee <dev@shortepic.com>")
        .about("AWS S3-backed infinitely-expandable mountable filesystem.")
        .arg(Arg::with_name("TARGETPATH")
            .default_value("/tmp/fs")
            // .required(true)
            .help("path to the test filesystem"))
        .arg(Arg::with_name("ACTUALPATH")
            .default_value("/tmp/back")
            // .required(true)
            .help("path under the filesystem"))
        .arg(Arg::with_name("EXPECTEDPATH")
            .default_value("/tmp/check")
            // .required(true)
            .help("path to store correct results"))
        .arg(Arg::with_name("times")
            .short("n")
            .long("times")
            .value_name("TIMES")
            .takes_value(true)
            .default_value("10")
            .help("number of writes to process"))
        .arg(Arg::with_name("checks")
            .short("c")
            .long("checkmod")
            .value_name("CHECKMOD")
            .takes_value(true)
            .default_value("10")
            .help("frequency to perform checks"))
        .arg(Arg::with_name("seed")
            .short("s")
            .long("seed")
            .value_name("SEED")
            .takes_value(true)
            .help("seed for rng which will ensure repeatable sequence"));

    let cmdline = app.get_matches();
    let targetpath = cmdline.value_of("TARGETPATH").unwrap();
    let actualpath = cmdline.value_of("ACTUALPATH").unwrap();
    let expectedpath = cmdline.value_of("EXPECTEDPATH").unwrap();
    let times = cmdline.value_of("times").unwrap().parse::<i32>().unwrap();
    let checks = cmdline.value_of("checks").unwrap().parse::<i32>().unwrap();
    let mut rng: StdRng = StdRng::new().unwrap();
    if let Some(seed) = cmdline.value_of("seed") {
        let i: Vec<usize> = seed.bytes().map(|xx| xx as usize).collect();
        rng.reseed(&i);
    }

    trace!("{:?}", cmdline);
    trace!("{:?}", checks);

    // hit the target and expected many times
    for i in 0..times {
        if i % checks == 0 {
            validate(actualpath, expectedpath);
        }

        write_random(targetpath, expectedpath, &mut rng);
    }
    validate(actualpath, expectedpath);

    Ok(())
}

fn write_random(targetpath: &str, expectedpath: &str, rng: &mut StdRng) -> Result<()> {
    use rand::Rng;

    let choices: Vec<u32> = (0..24).collect();
    let v = rng.choose(&choices).unwrap();
    println!("{:?}", v);
    Ok(())
}

fn validate(actualpath: &str, expectedpath: &str) -> Result<()> {
    println!("validating");
    Ok(())
}
