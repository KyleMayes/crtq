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

#![cfg_attr(feature="valgrind", feature(alloc_system))]

#[cfg(feature="valgrind")]
extern crate alloc_system;

#[macro_use]
extern crate queuecheck;
extern crate crtq;

use crtq::{Consumer, Producer};

#[cfg(feature="valgrind")]
const OPERATIONS: usize = 100_000;
#[cfg(not(feature="valgrind"))]
const OPERATIONS: usize = 100_000_000;

fn main() {
    let (producer, consumer) = crtq::channel(1, 1);
    queuecheck_test!(
        OPERATIONS,
        vec![producer.clone(), producer],
        vec![consumer.clone(), consumer],
        |p: &Producer<String>, i: String| p.produce(i).unwrap(),
        |c: &Consumer<String>| c.consume().ok()
    );
}
