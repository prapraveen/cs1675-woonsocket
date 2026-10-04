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
    // Use std here so diagnostics include any quanta initialization time.
    let process_start = std::time::Instant::now();
    log_event(process_start, "Server main entered");
    let args = ServerArgs::parse();
    log_event(
        process_start,
        &format!("Arguments parsed; runtime={}s", args.runtime_secs),
    );
    let deadline = Instant::now() + Duration::from_secs(args.runtime_secs);
    log_event(process_start, "Runtime timer started at startup");

    let listener = TcpListener::bind(("0.0.0.0", args.port))?;
    listener.set_nonblocking(true)?;
    println!("Listening on port {}", args.port);
    log_event(
        process_start,
        "Listener ready; waiting for first connection",
    );
    let mut connections = Vec::new();

    while Instant::now() < deadline {
        match listener.accept() {
            Ok((stream, _)) => {
                if connections.is_empty() {
                    log_event(process_start, "First connection accepted");
                }
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

    if Instant::now() >= deadline {
        log_event(process_start, "Runtime deadline reached");
    }
    log_event(
        process_start,
        "Stopping listener and shutting down connection sockets",
    );
    drop(listener);
    // Wake workers blocked in reads or writes before waiting for them.
    for (stream, _) in &connections {
        let _ = stream.shutdown(Shutdown::Both);
    }
    log_event(process_start, "Joining connection workers");
    let mut worker_panicked = false;
    for (_, worker) in connections {
        worker_panicked |= worker.join().is_err();
    }
    println!("Server shut down");
    log_event(
        process_start,
        "All connection workers joined; server exiting",
    );
    if worker_panicked {
        return Err(io::Error::other("connection worker panicked"));
    }
    Ok(())
}

fn log_event(start: std::time::Instant, message: &str) {
    eprintln!("[elapsed={:.3}s] {message}", start.elapsed().as_secs_f64());
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
