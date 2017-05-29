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
    use rand::Rng;

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
        .arg(Arg::with_name("seed")
            .short("s")
            .long("seed")
            .value_name("SEED")
            .takes_value(true)
            .help("seed for rng which will ensure repeatable sequence"));

    let cmdline = app.get_matches();
    let _targetpath = cmdline.value_of("TARGETPATH").unwrap();
    let _actualpath = cmdline.value_of("ACTUALPATH").unwrap();
    let _expectedpath = cmdline.value_of("EXPECTEDPATH").unwrap();
    let _times = cmdline.value_of("times").unwrap().parse::<i32>().unwrap();
    let mut rng: StdRng = StdRng::new().unwrap();
    if let Some(seed) = cmdline.value_of("seed") {
        let i: Vec<usize> = seed.bytes().map(|xx| xx as usize).collect();
        rng.reseed(&i);
    }

    trace!("{:?}", cmdline);

    // hit the target and expected many times
    for i in 0.._times {
        let choices: Vec<u32> = (0..24).collect();
        let v = rng.choose(&choices).unwrap();
        println!("{:?}", v);
    }


    // check the actuals against the expected



    Ok(())
}
