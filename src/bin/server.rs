use clap::Parser;
use quanta::Instant;
use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::Duration,
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
    let mut deadline = None;

    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    listener.set_nonblocking(true)?;
    println!("Listening on port {}", args.port);
    let mut connections = Vec::new();

    while deadline.is_none_or(|end| Instant::now() < end) {
        match listener.accept() {
            Ok((stream, _)) => {
                // All workers share the timer started by the first connection.
                let deadline = *deadline.get_or_insert_with(|| {
                    println!("First connection accepted; starting runtime timer");
                    Instant::now() + Duration::from_secs(args.runtime_secs)
                });
                // Keep a handle so main can unblock this worker at shutdown.
                let shutdown_stream = match stream.try_clone() {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("Error cloning connection: {e}");
                        continue;
                    }
                };
                let worker = thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, deadline) {
                        if Instant::now() < deadline {
                            eprintln!("Connection error: {e}");
                        }
                    }
                });
                connections.push((shutdown_stream, worker));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("Error accepting connection: {e}");
                break;
            }
        };
    }

    drop(listener);
    // Wake workers blocked in reads or writes before waiting for them.
    for (stream, _) in &connections {
        let _ = stream.shutdown(Shutdown::Both);
    }
    let mut worker_panicked = false;
    for (_, worker) in connections {
        worker_panicked |= worker.join().is_err();
    }
    println!("Server shut down");
    if worker_panicked {
        return Err(io::Error::other("connection worker panicked"));
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream, deadline: Instant) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    while Instant::now() < deadline {
        let request = woonsocket::read_request(&mut stream)?;
        if Instant::now() >= deadline {
            break;
        }
        let start = Instant::now();
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
    Ok(())
}
