use quanta::Instant;
use std::{
    io,
    net::{Shutdown, TcpStream},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

// Shared shutdown state; the mutex is used only when stopping, not per request.
pub(crate) struct ClientRun {
    pub start: Instant,
    pub deadline: Instant,
    stopped: AtomicBool,
    end: Mutex<Option<(Instant, String)>>,
    sockets: Vec<TcpStream>,
}

impl ClientRun {
    pub fn new(start: Instant, runtime: Duration, sockets: Vec<TcpStream>) -> Self {
        Self {
            start,
            deadline: start + runtime,
            stopped: AtomicBool::new(false),
            end: Mutex::new(None),
            sockets,
        }
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn stop(&self, at: Instant, reason: String) {
        let mut end = self.end.lock().unwrap();
        if end.is_some() {
            return;
        }
        *end = Some((at, reason));
        self.stopped.store(true, Ordering::Release);
        for socket in &self.sockets {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    pub fn handle_result(&self, result: io::Result<()>) -> io::Result<()> {
        if let Err(error) = result {
            let now = Instant::now();
            if self.stopped() {
                return Ok(());
            }
            if now >= self.deadline {
                self.stop(self.deadline, "runtime_elapsed".into());
                return Ok(());
            }
            let disconnected = matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
            );
            self.stop(
                now,
                format!(
                    "{}: {error}",
                    if disconnected {
                        "peer_disconnected"
                    } else {
                        "error"
                    }
                ),
            );
            if !disconnected {
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn wait(&self) {
        while !self.stopped() && Instant::now() < self.deadline {
            thread::sleep(Duration::from_millis(1));
        }
        self.stop(self.deadline, "runtime_elapsed".into());
    }

    pub fn outcome(&self) -> (u64, String) {
        let end = self.end.lock().unwrap();
        let (at, reason) = end.as_ref().unwrap();
        (
            at.saturating_duration_since(self.start).as_nanos() as u64,
            reason.clone(),
        )
    }
}
