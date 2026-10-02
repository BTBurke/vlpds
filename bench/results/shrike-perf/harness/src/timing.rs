//! A small interleaved benchmark harness.
//!
//! Each comparison runs `rounds` rounds. In every round each implementation
//! runs one timed batch (sized to `target` wall time by a calibration pass),
//! and the order of the implementations alternates between rounds, so slow
//! drift on a shared machine (thermal, other processes) hits both sides
//! alike. Reported: the median ns/op per implementation, the min, and the
//! median of the per-round ratios (shrike / other), with its min..max spread.

use std::hint::black_box;
use std::io::Write;
use std::time::{Duration, Instant};

pub struct Opts {
    pub rounds: usize,
    pub target: Duration,
    pub filter: Vec<String>,
    pub out: Option<std::fs::File>,
}

impl Opts {
    pub fn wants(&self, name: &str) -> bool {
        self.filter.is_empty() || self.filter.iter().any(|f| name.contains(f.as_str()))
    }
}

pub struct Row {
    pub group: String,
    pub case: String,
    pub unit: String,
    /// per-round ns per unit for (shrike, other)
    pub a: Vec<f64>,
    pub b: Vec<f64>,
    pub a_label: String,
    pub b_label: String,
}

pub fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 { s[n / 2] } else { (s[n / 2 - 1] + s[n / 2]) / 2.0 }
}

fn min(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn max(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
}

pub fn fmt_ns(ns: f64) -> String {
    if ns >= 1e9 {
        format!("{:.3} s", ns / 1e9)
    } else if ns >= 1e6 {
        format!("{:.2} ms", ns / 1e6)
    } else if ns >= 1e3 {
        format!("{:.2} µs", ns / 1e3)
    } else {
        format!("{:.1} ns", ns)
    }
}

/// Iterations so that one batch takes about `target`.
fn calibrate(f: &mut dyn FnMut(), target: Duration) -> u64 {
    f(); // warm up (caches, lazy statics, allocator)
    let mut n: u64 = 1;
    loop {
        let t = Instant::now();
        for _ in 0..n {
            f();
        }
        let e = t.elapsed();
        if e >= target / 8 || n >= 1 << 32 {
            let per = e.as_secs_f64() / n as f64;
            return ((target.as_secs_f64() / per.max(1e-12)).ceil() as u64).max(1);
        }
        n *= 2;
    }
}

fn batch(f: &mut dyn FnMut(), iters: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_nanos() as f64 / iters as f64
}

/// Interleaved comparison of `a` (shrike) and `b` (the other side, if any).
/// `per` divides each op's time (e.g. records per op) for ns/unit.
#[allow(clippy::too_many_arguments)]
pub fn compare(
    o: &mut Opts,
    group: &str,
    case: &str,
    unit: &str,
    per: f64,
    a_label: &str,
    a: &mut dyn FnMut(),
    b_label: &str,
    mut b: Option<&mut dyn FnMut()>,
) -> Option<Row> {
    let name = format!("{group}/{case}");
    if !o.wants(&name) {
        return None;
    }
    let na = calibrate(a, o.target);
    let nb = b.as_mut().map(|b| calibrate(*b, o.target)).unwrap_or(0);
    let (mut ra, mut rb) = (Vec::new(), Vec::new());
    for r in 0..o.rounds {
        let first_a = r % 2 == 0;
        for side in 0..2 {
            let run_a = (side == 0) == first_a;
            if run_a {
                ra.push(batch(a, na) / per);
            } else if let Some(b) = b.as_mut() {
                rb.push(batch(*b, nb) / per);
            }
        }
    }
    let row = Row {
        group: group.into(),
        case: case.into(),
        unit: unit.into(),
        a: ra,
        b: rb,
        a_label: a_label.into(),
        b_label: b_label.into(),
    };
    report(o, &row);
    Some(row)
}

/// Like [`compare`] for operations too slow (or too stateful) to batch:
/// each closure does its own setup and returns the measured time of one
/// run, divided by `per`.
#[allow(clippy::too_many_arguments)]
pub fn oneshot(
    o: &mut Opts,
    rounds: usize,
    group: &str,
    case: &str,
    unit: &str,
    a_label: &str,
    a: &mut dyn FnMut() -> (Duration, f64),
    b_label: &str,
    mut b: Option<&mut dyn FnMut() -> (Duration, f64)>,
) -> Option<Row> {
    let name = format!("{group}/{case}");
    if !o.wants(&name) {
        return None;
    }
    // one warm-up run each
    black_box(a());
    if let Some(b) = b.as_mut() {
        black_box(b());
    }
    let (mut ra, mut rb) = (Vec::new(), Vec::new());
    for r in 0..rounds {
        let first_a = r % 2 == 0;
        for side in 0..2 {
            let run_a = (side == 0) == first_a;
            if run_a {
                let (d, per) = a();
                ra.push(d.as_nanos() as f64 / per);
            } else if let Some(b) = b.as_mut() {
                let (d, per) = b();
                rb.push(d.as_nanos() as f64 / per);
            }
        }
    }
    let row = Row {
        group: group.into(),
        case: case.into(),
        unit: unit.into(),
        a: ra,
        b: rb,
        a_label: a_label.into(),
        b_label: b_label.into(),
    };
    report(o, &row);
    Some(row)
}

fn report(o: &mut Opts, r: &Row) {
    let ma = median(&r.a);
    let line = if r.b.is_empty() {
        format!(
            "{:<14} {:<52} {:>12} (min {:>10})  [{}]  per {}",
            r.group,
            r.case,
            fmt_ns(ma),
            fmt_ns(min(&r.a)),
            r.a_label,
            r.unit
        )
    } else {
        let mb = median(&r.b);
        let ratios: Vec<f64> = r.a.iter().zip(&r.b).map(|(a, b)| a / b).collect();
        format!(
            "{:<14} {:<52} {:>12} vs {:>12}  ratio {:>6.2}x (rounds {:.2}..{:.2})  [{} vs {}]  per {}",
            r.group,
            r.case,
            fmt_ns(ma),
            fmt_ns(mb),
            median(&ratios),
            min(&ratios),
            max(&ratios),
            r.a_label,
            r.b_label,
            r.unit
        )
    };
    println!("{line}");
    if let Some(f) = o.out.as_mut() {
        let j = serde_json::json!({
            "group": r.group, "case": r.case, "unit": r.unit,
            "a_label": r.a_label, "b_label": r.b_label,
            "a_ns": r.a, "b_ns": r.b,
            "a_median": ma, "b_median": if r.b.is_empty() { f64::NAN } else { median(&r.b) },
        });
        let _ = writeln!(f, "{j}");
    }
}

pub fn report_pub(o: &mut Opts, r: &Row) {
    report(o, r)
}
