use std::{
    fs::{self, File},
    io::{self, BufWriter, Write},
    net::{SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use quanta::Instant;
use woonsocket_work::Work;
use woonsocket_work::args::WoonsocketClientOpt;

use crate::client_run::ClientRun;
use crate::{Request, read_response, write_request};

struct LatencyRecord {
    worker_id: u64,
    request_id: u64,
    start_ns: u64,
    finish_ns: u64,
    server_processing_time_ns: u64,
}

#[derive(Default)]
#[repr(align(128))]
struct RunLog {
    records: Vec<LatencyRecord>,
    attempted: Vec<u64>,
    sent: Vec<u64>,
}

pub fn run(args: &WoonsocketClientOpt, num_threads: u64) -> io::Result<()> {
    if num_threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "num-threads must be positive",
        ));
    }
    fs::create_dir_all(&args.outpath)?;
    let address = SocketAddr::from((args.ip, args.port));
    let mut connections = Vec::new();

    for _ in 0..num_threads {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        stream.set_nodelay(true)?;
        connections.push(stream);
    }

    let mut worker_logs: Vec<RunLog> = connections.iter().map(|_| RunLog::default()).collect();
    let start = Instant::now();
    let runtime = Duration::from_secs(args.runtime_secs);
    let control = ClientRun::new(
        start,
        runtime,
        connections
            .iter()
            .map(TcpStream::try_clone)
            .collect::<io::Result<Vec<_>>>()?,
    );
    let control = &control;
    let mut first_error = None;

    // spawn workers which write to the worker logs vector
    thread::scope(|scope| {
        let mut workers = Vec::new();

        for (worker_id, (stream, log)) in connections
            .into_iter()
            .zip(worker_logs.iter_mut())
            .enumerate()
        {
            workers.push(scope.spawn(move || {
                control.handle_result(run_worker(
                    stream,
                    worker_id as u64,
                    num_threads,
                    args.work,
                    start,
                    runtime,
                    log,
                    control,
                ))
            }));
        }
        control.wait();
        for worker in workers {
            let result = worker
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("client worker panicked")));
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
    });

    let (measurement_duration_ns, termination_reason) = control.outcome();
    // Aggregate and write logs
    let mut logs = RunLog::default();
    for log in worker_logs {
        logs.records.extend(log.records);
        logs.attempted.extend(log.attempted);
        logs.sent.extend(log.sent);
    }
    logs.records
        .retain(|record| record.finish_ns < measurement_duration_ns);
    let attempted = logs
        .attempted
        .iter()
        .filter(|&&time| time < measurement_duration_ns)
        .count();
    let sent = logs
        .sent
        .iter()
        .filter(|&&time| time < measurement_duration_ns)
        .count();
    logs.records.sort_unstable_by_key(|record| record.start_ns);
    let mut file = BufWriter::new(File::create(args.outpath.join("closed_loop.csv"))?);
    writeln!(
        file,
        "worker_id,request_id,start_ns,finish_ns,latency_ns,server_processing_time_ns"
    )?;
    for record in &logs.records {
        writeln!(
            file,
            "{},{},{},{},{},{}",
            record.worker_id,
            record.request_id,
            record.start_ns,
            record.finish_ns,
            record.finish_ns - record.start_ns,
            record.server_processing_time_ns
        )?;
    }
    file.flush()?;

    let duration_secs = measurement_duration_ns as f64 / 1e9;
    let summary = serde_json::json!({
        "num_threads": num_threads,
        "runtime_secs": args.runtime_secs,
        "measurement_duration_ns": measurement_duration_ns,
        "offered_rps": (duration_secs > 0.0).then(|| sent as f64 / duration_secs),
        "achieved_rps": (duration_secs > 0.0).then(|| logs.records.len() as f64 / duration_secs),
        "termination_reason": termination_reason,
        "attempted": attempted,
        "sent": sent,
        "completed": logs.records.len(),
        "unfinished": sent.saturating_sub(logs.records.len()),
        "error": first_error.as_ref().map(ToString::to_string),
    });
    fs::write(
        args.outpath.join("closed_loop_summary.json"),
        summary.to_string(),
    )?;

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn run_worker(
    mut stream: TcpStream,
    worker_id: u64,
    num_threads: u64,
    work: Work,
    start: Instant,
    runtime: Duration,
    log: &mut RunLog,
    control: &ClientRun,
) -> io::Result<()> {
    let mut request_id = worker_id;

    // Save completed records even if a later exchange fails or times out.
    let result = (|| -> io::Result<()> {
        while !control.stopped() && start.elapsed() < runtime {
            let remaining = runtime.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }
            stream.set_read_timeout(Some(remaining))?;
            stream.set_write_timeout(Some(remaining))?;

            let request_start = Instant::now();
            let generated_ns = request_start.duration_since(start).as_nanos() as u64;
            log.attempted.push(generated_ns);
            let request = Request {
                request_id,
                scheduled_ns: generated_ns,
                generated_ns,
                work,
            };
            write_request(&mut stream, &request)?;
            log.sent.push(start.elapsed().as_nanos() as u64);
            let response = read_response(&mut stream)?;
            let finish = Instant::now();
            if response.request_id != request_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response request ID mismatch",
                ));
            }
            log.records.push(LatencyRecord {
                worker_id,
                request_id,
                start_ns: request_start.duration_since(start).as_nanos() as u64,
                finish_ns: finish.duration_since(start).as_nanos() as u64,
                server_processing_time_ns: response.server_processing_time,
            });
            // Interleave IDs between workers without a shared counter.
            request_id += num_threads;
        }
        Ok(())
    })();

    match result {
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) && start.elapsed() >= runtime =>
        {
            Ok(())
        }
        other => other,
    }
}
