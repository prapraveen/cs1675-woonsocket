use std::io;

use clap::Parser;
use woonsocket::{closed_loop, open_loop};
use woonsocket_work::args::{ClientMode, WoonsocketClientOpt};

fn main() -> io::Result<()> {
    let args = WoonsocketClientOpt::parse();

    match &args.mode {
        ClientMode::ClosedLoop { num_threads } => closed_loop::run(&args, *num_threads),
        ClientMode::OpenLoop { interval_us, kind } => open_loop::run(&args, *interval_us, kind),
    }
}
