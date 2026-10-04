use std::{
    fs::{self, File},
    hint::spin_loop,
    io::{self, BufWriter, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use quanta::Instant;
use rand_distr::{Distribution, Exp};
use woonsocket_work::{
    Work,
    args::{OpenLoopKind, WoonsocketClientOpt},
};

use crate::{Request, Response, read_response, write_request};

struct SendRecord {
    request_id: u64,
    scheduled_ns: u64,
    generated_ns: u64,
    sent_ns: Option<u64>,
}

struct ReceiveRecord {
    response: Response,
    completed_ns: u64,
}

#[derive(Default)]
#[repr(align(128))]
struct SendLog {
    records: Vec<SendRecord>,
}

#[derive(Default)]
#[repr(align(128))]
struct ReceiveLog {
    records: Vec<ReceiveRecord>,
}

pub fn run(args: &WoonsocketClientOpt, interval_us: u64, kind: &OpenLoopKind) -> io::Result<()> {
    let num_workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let interval_ns = interval_us
        .checked_mul(1000)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "interval-us must be positive and fit in nanoseconds",
            )
        })?;
    let runtime_ns = args
        .runtime_secs
        .checked_mul(1_000_000_000)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "runtime is too large"))?;
    fs::create_dir_all(&args.outpath)?;
    let address = SocketAddr::from((args.ip, args.port));
    let mut connections = Vec::new();
    let mut shutdown_handles = Vec::new();
    for _ in 0..num_workers {
        let sender = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        sender.set_nodelay(true)?;
        let receiver = sender.try_clone()?;
        shutdown_handles.push(sender.try_clone()?);
        connections.push((sender, receiver));
    }

    let mut send_logs: Vec<SendLog> = (0..num_workers).map(|_| SendLog::default()).collect();
    let mut receive_logs: Vec<ReceiveLog> =
        (0..num_workers).map(|_| ReceiveLog::default()).collect();

    let start = Instant::now() + Duration::from_millis(100);
    let deadline = start + Duration::from_nanos(runtime_ns);
    let mut first_error = None;
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for (worker_id, ((sender, receiver), (send_log, receive_log))) in connections
            .into_iter()
            .zip(send_logs.iter_mut().zip(receive_logs.iter_mut()))
            .enumerate()
        {
            handles.push(scope.spawn(move || {
                send_requests(
                    sender,
                    worker_id as u64,
                    num_workers as u64,
                    args.work,
                    interval_ns,
                    kind,
                    start,
                    deadline,
                    send_log,
                )
            }));
            handles.push(scope.spawn(move || {
                receive_responses(
                    receiver,
                    worker_id as u64,
                    num_workers as u64,
                    start,
                    deadline,
                    receive_log,
                )
            }));
        }

        while Instant::now() < deadline {
            thread::sleep(deadline.saturating_duration_since(Instant::now()));
        }
        for stream in &shutdown_handles {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for handle in handles {
            let result = handle
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("open-loop worker panicked")));
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
    });

    let mut file = BufWriter::new(File::create(args.outpath.join("open_loop.csv"))?);
    writeln!(
        file,
        "worker_id,request_id,scheduled_ns,generated_ns,sent_ns,completed_ns,generator_lateness_ns,latency_ns,scheduled_latency_ns,server_processing_time_ns"
    )?;
    let mut generated = 0u64;
    let mut sent = 0u64;
    let mut completed = 0u64;
    let mut writes_finished_after_deadline = 0u64;
    for (worker_id, (send_log, receive_log)) in send_logs.iter().zip(&receive_logs).enumerate() {
        let mut responses = receive_log.records.iter().peekable();
        for request in &send_log.records {
            generated += 1;
            if let Some(time) = request.sent_ns {
                if time < runtime_ns {
                    sent += 1;
                } else {
                    writes_finished_after_deadline += 1;
                }
            }
            let response = if responses
                .peek()
                .is_some_and(|r| r.response.request_id == request.request_id)
            {
                responses.next()
            } else {
                None
            };
            if response.is_some() {
                completed += 1;
            }
            let optional = |value: Option<u64>| value.map(|v| v.to_string()).unwrap_or_default();
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{}",
                worker_id,
                request.request_id,
                request.scheduled_ns,
                request.generated_ns,
                optional(request.sent_ns),
                optional(response.map(|r| r.completed_ns)),
                request.generated_ns - request.scheduled_ns,
                optional(response.map(|r| r.completed_ns - r.response.generated_ns)),
                optional(response.map(|r| r.completed_ns - r.response.scheduled_ns)),
                optional(response.map(|r| r.response.server_processing_time))
            )?;
        }
    }
    file.flush()?;
    // Online Poisson sampling does not enumerate arrivals beyond where a stalled
    // sender stopped. Report the expectation, not a fabricated exact count.
    let summary = serde_json::json!({
        "kind": match kind { OpenLoopKind::Constant => "constant", OpenLoopKind::Poisson => "poisson" },
        "num_workers": num_workers,
        "runtime_secs": args.runtime_secs, "interval_us": interval_us,
        "expected_scheduled": runtime_ns as f64 / interval_ns as f64,
        "generated": generated, "sent_before_deadline": sent,
        "writes_finished_after_deadline": writes_finished_after_deadline,
        "completed": completed, "generated_without_response": generated - completed,
        "error": first_error.as_ref().map(ToString::to_string),
    });
    fs::write(
        args.outpath.join("open_loop_summary.json"),
        summary.to_string(),
    )?;
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn send_requests(
    mut stream: TcpStream,
    worker_id: u64,
    num_workers: u64,
    work: Work,
    interval_ns: u64,
    kind: &OpenLoopKind,
    start: Instant,
    deadline: Instant,
    log: &mut SendLog,
) -> io::Result<()> {
    let interval = Duration::from_nanos(interval_ns.checked_mul(num_workers).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "per-worker interval is too large",
        )
    })?);
    // Each sender owns its RNG; gaps are durations rather than raw nanoseconds.
    let mut rng = rand::thread_rng();
    let exponential = Exp::new(1.0 / interval.as_secs_f64()).unwrap();
    let mut gap = || match kind {
        OpenLoopKind::Constant => interval,
        OpenLoopKind::Poisson => Duration::from_secs_f64(exponential.sample(&mut rng)),
    };
    let mut request_id = worker_id;
    let offset = match kind {
        OpenLoopKind::Constant => Duration::from_nanos(worker_id * interval_ns),
        OpenLoopKind::Poisson => gap(),
    };
    // Cap at the deadline so very long intervals never overflow an Instant.
    let mut next_send = start + offset;
    while next_send < deadline {
        while Instant::now() < next_send {
            spin_loop();
        }
        let generated = Instant::now();
        if generated >= deadline {
            return Ok(());
        }
        let scheduled_ns = next_send.duration_since(start).as_nanos() as u64;
        let generated_ns = generated.duration_since(start).as_nanos() as u64;
        let request = Request {
            request_id,
            scheduled_ns,
            generated_ns,
            work,
        };
        log.records.push(SendRecord {
            request_id,
            scheduled_ns,
            generated_ns,
            sent_ns: None,
        });
        match write_request(&mut stream, &request) {
            Ok(()) => {
                log.records.last_mut().unwrap().sent_ns = Some(start.elapsed().as_nanos() as u64)
            }
            Err(_) if Instant::now() >= deadline => return Ok(()),
            Err(error) => {
                let _ = stream.shutdown(Shutdown::Both);
                return Err(error);
            }
        }
        // Advance from the previous scheduled time, not the actual send time.
        request_id += num_workers;
        next_send += gap().min(deadline.duration_since(next_send));
    }
    Ok(())
}

fn receive_responses(
    mut stream: TcpStream,
    worker_id: u64,
    num_workers: u64,
    start: Instant,
    deadline: Instant,
    log: &mut ReceiveLog,
) -> io::Result<()> {
    let mut expected_id = worker_id;
    while Instant::now() < deadline {
        let response = match read_response(&mut stream) {
            Ok(response) => response,
            Err(_) if Instant::now() >= deadline => return Ok(()),
            Err(error) => {
                let _ = stream.shutdown(Shutdown::Both);
                return Err(error);
            }
        };
        let finish = Instant::now();
        if finish >= deadline {
            break;
        }
        let completed_ns = finish.duration_since(start).as_nanos() as u64;
        if response.request_id != expected_id
            || response.scheduled_ns > response.generated_ns
            || response.generated_ns > completed_ns
        {
            let _ = stream.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid response ID or client timestamps",
            ));
        }
        expected_id += num_workers;
        // Keep timing metadata only; do not retain payload allocations in logs.
        log.records.push(ReceiveRecord {
            response: Response {
                payload: Vec::new(),
                ..response
            },
            completed_ns,
        });
    }
    Ok(())
}
