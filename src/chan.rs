//! The repo workers' request channel: unbounded MPSC, a mutex-guarded queue
//! and a condition variable. An idle receiver parks right away (crossbeam's
//! `recv` spins and yields first: ~1.6% of a node's CPU at saturation on
//! benchbox, workers waking and idling between batches), and it takes a whole
//! batch per lock (`recv_batch`) instead of one atomic hand-off per message.

use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct State<T> {
    queue: VecDeque<T>,
    senders: usize,
    receiver: bool,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
}

pub struct Sender<T>(Arc<Inner<T>>);

pub struct Receiver<T>(Arc<Inner<T>>);

/// The receiver is gone; the message is handed back.
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sending on a disconnected channel")
    }
}

impl<T> std::error::Error for SendError<T> {}

#[derive(Debug, PartialEq, Eq)]
pub enum RecvError {
    /// No message within the timeout.
    Timeout,
    /// Every sender is gone and the queue is empty.
    Disconnected,
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner { state: Mutex::new(State { queue: VecDeque::new(), senders: 1, receiver: true }), ready: Condvar::new() });
    (Sender(inner.clone()), Receiver(inner))
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let mut s = self.0.state.lock();
        if !s.receiver {
            return Err(SendError(msg));
        }
        s.queue.push_back(msg);
        drop(s);
        self.0.ready.notify_one();
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.state.lock().senders += 1;
        Sender(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut s = self.0.state.lock();
        s.senders -= 1;
        if s.senders == 0 {
            drop(s);
            self.0.ready.notify_all();
        }
    }
}

impl<T> Receiver<T> {
    /// Waits for at least one message (up to `timeout`, if given), then
    /// moves up to `max` queued messages into `out`.
    pub fn recv_batch(&self, out: &mut Vec<T>, max: usize, timeout: Option<Duration>) -> Result<(), RecvError> {
        let deadline = timeout.map(|t| Instant::now() + t);
        let mut s = self.0.state.lock();
        while s.queue.is_empty() {
            if s.senders == 0 {
                return Err(RecvError::Disconnected);
            }
            match deadline {
                None => self.0.ready.wait(&mut s),
                Some(d) => {
                    if self.0.ready.wait_until(&mut s, d).timed_out() && s.queue.is_empty() {
                        return Err(if s.senders == 0 { RecvError::Disconnected } else { RecvError::Timeout });
                    }
                }
            }
        }
        let n = s.queue.len().min(max.max(1));
        out.extend(s.queue.drain(..n));
        Ok(())
    }

    /// Messages queued.
    pub fn len(&self) -> usize {
        self.0.state.lock().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut s = self.0.state.lock();
        s.receiver = false;
        let queued = std::mem::take(&mut s.queue);
        drop(s);
        drop(queued);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_timeouts_and_disconnects() {
        let (tx, rx) = unbounded::<u32>();
        let mut out = Vec::new();
        assert_eq!(rx.recv_batch(&mut out, 8, Some(Duration::from_millis(5))), Err(RecvError::Timeout));
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        assert_eq!(rx.len(), 5);
        rx.recv_batch(&mut out, 3, None).unwrap();
        assert_eq!(out, [0, 1, 2]);
        rx.recv_batch(&mut out, 8, None).unwrap();
        assert_eq!(out, [0, 1, 2, 3, 4]);
        // a parked receiver wakes on a send from another thread
        let tx2 = tx.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            tx2.send(9).unwrap();
        });
        out.clear();
        rx.recv_batch(&mut out, 8, None).unwrap();
        assert_eq!(out, [9]);
        t.join().unwrap();
        // the last sender gone: queued messages first, then Disconnected
        tx.send(7).unwrap();
        drop(tx);
        out.clear();
        rx.recv_batch(&mut out, 8, None).unwrap();
        assert_eq!(out, [7]);
        assert_eq!(rx.recv_batch(&mut out, 8, None), Err(RecvError::Disconnected));
        assert_eq!(rx.recv_batch(&mut out, 8, Some(Duration::from_millis(1))), Err(RecvError::Disconnected));
        // a dropped receiver refuses sends
        let (tx, rx) = unbounded::<u32>();
        drop(rx);
        assert_eq!(tx.send(1).unwrap_err().0, 1);
    }
}
