use clap::Parser;
use std::{
    io,
    net::{TcpListener, TcpStream},
    path::PathBuf,
};

#[derive(Debug, Parser)]
struct ServerArgs {
    #[arg(short = 'p', long)]
    port: u16,

    #[arg(short = 'r', long)]
    runtime_secs: u64,

    #[arg(short = 'o', long)]
    outpath: PathBuf,
}

fn main() -> io::Result<()> {
    let args = ServerArgs::parse();

    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    println!("Listening on port {}", args.port);

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                std::thread::spawn(move || match handle_connection(stream) {
                    Ok(_) => {}
                    Err(e) => eprintln!("Connection error: {e}"),
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {e}");
            }
        };
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    loop {
        let request = woonsocket::read_request(&mut stream)?;
        let start = quanta::Instant::now();
        let payload = request.work.perform();
        let server_processing_time = start.elapsed().as_nanos() as u64;

        let response = woonsocket::Response {
            request_id: request.request_id,
            scheduled_ns: request.scheduled_ns,
            generated_ns: request.generated_ns,
            server_processing_time,
            payload: payload.unwrap_or_default(),
        };
        woonsocket::write_response(&mut stream, &response)?;
    }
}
