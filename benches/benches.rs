// Copyright 2017 Kyle Mayes
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_use]
extern crate queuecheck;
extern crate crtq;

use crtq::{Consumer, Producer};

use queuecheck::{Latency};

const RUNS: usize = 5;
const WARMUP: usize = 1_000_000;
const MEASUREMENT: usize = 100_000_000;
const RANKS: &[f64] = &[50.0, 70.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999, 99.9999, 99.99999];

fn thousands(ops: f64) -> String {
    let mut string = format!("{:.2}", ops);
    let mut index = string.find('.').unwrap();
    while index > 3 {
        index -= 3;
        string.insert(index, '_');
    }
    string
}

fn throughput() -> f64 {
    let (producer, consumer) = crtq::channel(0, 0);
    queuecheck_bench_throughput!(
        (WARMUP, MEASUREMENT),
        vec![producer],
        vec![consumer],
        |p: &Producer<i32>, i: i32| p.produce(i).unwrap(),
        |c: &Consumer<i32>| c.consume().ok()
    )
}

fn latency() -> Latency {
    let (producer, consumer) = crtq::channel(0, 0);
    queuecheck_bench_latency!(
        (WARMUP, MEASUREMENT),
        vec![producer],
        vec![consumer],
        |p: &Producer<i32>, i: i32| p.produce(i).unwrap(),
        |c: &Consumer<i32>| c.consume().ok()
    )
}

fn main() {
    println!("throughput_crtq");
    let mut runs = (0..RUNS).map(|_| throughput()).collect::<Vec<_>>();
    runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!("  {} operations/second\n", thousands(runs[RUNS / 2]));
    latency().report("latency_crtq", RANKS);
    println!();
}
