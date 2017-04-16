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

//! A multi-producer, multi-consumer wait-free queue.
//!
//! [A Wait-Free Queue with Wait-Free Memory Reclamation](https://github.com/pramalhe/ConcurrencyFreaks/blob/master/papers/crturnqueue-2016.pdf)

#![warn(missing_copy_implementations, missing_debug_implementations, missing_docs)]

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", warn(clippy))]

#![cfg_attr(feature="valgrind", feature(alloc_system))]

#[cfg(feature="valgrind")]
extern crate alloc_system;

extern crate hazard;

use std::error;
use std::fmt;
use std::ptr;
use std::usize;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::*;

use hazard::{AlignVec, BoxMemory, Memory, Pointers};

/// The number of pointers that fit in a 128 byte cacheline.
#[cfg(target_pointer_width="32")]
const POINTERS: usize = 32;
#[cfg(target_pointer_width="64")]
const POINTERS: usize = 16;

/// An invalid index.
const INVALID: usize = usize::MAX;

//================================================
// Enums
//================================================

// ConsumeError __________________________________

/// Indicates the reason a `consume` operation could not return an item.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConsumeError {
    /// The queue was empty and had no remaining producers.
    Disconnected,
    /// The queue was empty.
    Empty,
}

impl error::Error for ConsumeError {
    fn description(&self) -> &str {
        match *self {
            ConsumeError::Disconnected => "the queue was empty and had no remaining producers",
            ConsumeError::Empty => "the queue was empty",
        }
    }
}

impl fmt::Display for ConsumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}", error::Error::description(self))
    }
}

//================================================
// Structs
//================================================

// Consumer ______________________________________

/// A consumer for an unbounded MPMC wait-free queue.
#[derive(Debug)]
pub struct Consumer<T>(usize, Arc<Queue<T>>);

impl<T> Consumer<T> {
    //- Accessors --------------------------------

    /// Attempts to remove and return the item at the front of the queue.
    ///
    /// This method returns `Err` if the queue is empty.
    pub fn consume(&self) -> Result<T, ConsumeError> {
        self.1.consume(self.0)
    }

    /// Attempts to clone this consumer.
    pub fn try_clone(&self) -> Option<Self> {
        if let Some(thread) = self.1.deqthreads.lock().unwrap().pop() {
            self.1.consumers.fetch_add(1, Release);
            Some(Consumer(thread, self.1.clone()))
        } else {
            None
        }
    }
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        self.try_clone().expect("too many consumer clones")
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.1.deqthreads.lock().unwrap().push(self.0);
        self.1.consumers.fetch_sub(1, Release);
    }
}

unsafe impl<T> Send for Consumer<T> where T: Send { }

// Node __________________________________________

#[derive(Debug)]
struct Node<T> {
    /// The item contained in this node, if any.
    item: Option<T>,
    /// The enqueuer that created and enqueued this node.
    enqueuer: usize,
    /// The dequeuer that has dequeued or will dequeue this node.
    dequeuer: AtomicUsize,
    /// The next node in the singly-linked list.
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    //- Constructors -----------------------------

    fn new(item: Option<T>, enqueuer: usize) -> Self {
        let dequeuer = AtomicUsize::new(INVALID);
        Node { item, enqueuer, dequeuer, next: AtomicPtr::new(ptr::null_mut()) }
    }
}

unsafe impl<T> Send for Queue<T> where T: Send { }

// ProduceError __________________________________

/// Contains an item rejected by a `produce` operation.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ProduceError<T>(pub T);

impl<T> error::Error for ProduceError<T> {
    fn description(&self) -> &str {
        "the queue was full"
    }
}

impl<T> fmt::Debug for ProduceError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "ProduceError(..)")
    }
}

impl<T> fmt::Display for ProduceError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}", error::Error::description(self))
    }
}

// Producer __________________________________

/// A producer for an unbounded MPMC wait-free queue.
#[derive(Debug)]
pub struct Producer<T>(usize, Arc<Queue<T>>);

impl<T> Producer<T> {
    //- Accessors --------------------------------

    /// Attempts to add the supplied item to the back of the queue.
    ///
    /// This method returns `Err` if the queue is full or has no remaining consumers.
    pub fn produce(&self, item: T) -> Result<(), ProduceError<T>> {
        self.1.produce(self.0, item)
    }

    /// Attempts to clone this producer.
    pub fn try_clone(&self) -> Option<Self> {
        if let Some(thread) = self.1.enqthreads.lock().unwrap().pop() {
            self.1.producers.fetch_add(1, Release);
            Some(Producer(thread, self.1.clone()))
        } else {
            None
        }
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.try_clone().expect("too many producer clones")
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.1.enqthreads.lock().unwrap().push(self.0);
        self.1.producers.fetch_sub(1, Release);
    }
}

unsafe impl<T> Send for Producer<T> where T: Send { }

// Queue _________________________________________

const WRITE: usize = 0;
const READ: usize = 0;
const NEXT: usize = 1;
const DEQUEUE: usize = 2;

#[derive(Debug)]
#[repr(C)]
struct Queue<T> {
    /// The head of the queue (where nodes are inserted).
    write: AtomicPtr<Node<T>>,
    /// The number of active producers.
    producers: AtomicUsize,
    /// Padding to put the two fields above on their own cacheline.
    _wpadding: [usize; POINTERS - 2],

    /// Nodes which are waiting to be enqueued.
    enqreq: AlignVec<AtomicPtr<Node<T>>>,

    /// The tail of the queue (where nodes are removed).
    read: AtomicPtr<Node<T>>,
    /// The number of active consumers.
    consumers: AtomicUsize,
    /// Padding to put the two fields above on their own cacheline.
    _rpadding: [usize; POINTERS - 2],

    /// Nodes which are waiting to be dequeued.
    deqreq: AlignVec<AtomicPtr<Node<T>>>,
    /// Nodes which have been previously dequeued.
    deqold: AlignVec<AtomicPtr<Node<T>>>,

    /// Hazard pointers.
    pointers: Pointers<Node<T>, BoxMemory>,
    /// The empty sentinel node.
    sentinel: *mut Node<T>,

    /// Unused enqueuer thread indices.
    enqthreads: Mutex<Vec<usize>>,
    /// Unused dequeuer thread indices.
    deqthreads: Mutex<Vec<usize>>,
}

impl<T> Queue<T> {
    //- Constructors -----------------------------

    fn new(producers: usize, consumers: usize) -> Arc<Self> {
        let sentinel = BoxMemory.allocate(Node::new(None, 0));
        let enqreq = (0..producers).map(|_| AtomicPtr::new(ptr::null_mut())).collect();
        let deqold = (0..consumers).map(|_| {
            AtomicPtr::new(BoxMemory.allocate(Node::new(None, 0)))
        }).collect();
        let deqreq = (0..consumers).map(|_| {
            AtomicPtr::new(BoxMemory.allocate(Node::new(None, 0)))
        }).collect();
        Arc::new(Queue {
            write: AtomicPtr::new(sentinel),
            producers: AtomicUsize::new(1),
            _wpadding: [0; POINTERS - 2],
            enqreq: AlignVec::new(enqreq),
            read: AtomicPtr::new(sentinel),
            consumers: AtomicUsize::new(1),
            _rpadding: [0; POINTERS - 2],
            deqreq: AlignVec::new(deqreq),
            deqold: AlignVec::new(deqold),
            pointers: Pointers::new(BoxMemory, producers + consumers, 3, 512),
            sentinel,
            enqthreads: Mutex::new((1..producers).collect()),
            deqthreads: Mutex::new((1..consumers).collect()),
        })
    }

    //- Accessors --------------------------------

    /// Steps:
    ///
    /// 1. Signal intent to enqueue by storing a new node in `enqreq`
    /// 2. Set `write.next` to point to the new node
    /// 3. Advance `write` to point to `write.next`
    /// 4. Remove the newly enqueued node from `enqreq`
    ///
    /// Threads will attempt to enqueue nodes for other threads following the steps above until
    /// their own node has been enqueued.
    fn produce(&self, thread: usize, item: T) -> Result<(), ProduceError<T>> {
        // Return an error if all of the consumers have been disconnected.
        if self.consumers.load(Acquire) == 0 {
            return Err(ProduceError(item));
        }

        // Step 1.
        let node = BoxMemory.allocate(Node::new(Some(item), thread));
        self.enqreq[thread].store(node, SeqCst);

        for _ in 0..self.enqreq.len() {
            // If this thread's node has been enqueued, return.
            if self.enqreq[thread].load(SeqCst).is_null() {
                self.pointers.clear(thread, WRITE);
            }

            // Load the current write pointer.
            let write = self.pointers.mark_ptr(thread, WRITE, self.write.load(SeqCst));
            // If another thread has advanced the write pointer, skip this iteration.
            if write != self.write.load(SeqCst) {
                continue;
            }

            // Step 4.
            let enqueuer = unsafe { (*write).enqueuer };
            if self.enqreq[enqueuer].load(SeqCst) == write {
                exchange_ptr(&self.enqreq[enqueuer], write, ptr::null_mut());
            }

            // Step 2.
            for turn in 1..(self.enqreq.len() + 1) {
                let node = self.enqreq[(enqueuer + turn) % self.enqreq.len()].load(SeqCst);
                if !node.is_null() {
                    exchange_ptr(unsafe { &(*write).next }, ptr::null_mut(), node);
                    break;
                }
            }

            // Step 3.
            let next = unsafe { (*write).next.load(SeqCst) };
            if !next.is_null() {
                exchange_ptr(&self.write, write, next);
            }
        }

        // Step 4 again, just in case.
        self.enqreq[thread].store(ptr::null_mut(), Release);
        self.pointers.clear(thread, WRITE);
        Ok(())
    }

    /// Searches for the next active dequeue request and attempts to assign `next` to it.
    fn assign(&self, read: *mut Node<T>, next: *mut Node<T>) -> bool {
        let dequeuer = unsafe { (*read).dequeuer.load(SeqCst) };
        for turn in 1..(self.deqreq.len() + 1) {
            let thread = dequeuer.wrapping_add(turn) % self.deqreq.len();

            // If this thread does not have an active dequeue request, skip this iteration.
            if self.deqreq[thread].load(SeqCst) != self.deqold[thread].load(SeqCst) {
                continue;
            }

            // Attempt to assign `next` to this thread.
            let dequeuer = unsafe { (*next).dequeuer.load(SeqCst) };
            if dequeuer == INVALID {
                exchange_usize(unsafe { &(*next).dequeuer }, INVALID, thread);
            }

            break;
        }

        // Return whether `next` was assigned to
        let dequeuer = unsafe { (*next).dequeuer.load(SeqCst) };
        dequeuer != INVALID
    }

    /// Closes the dequeue request that `next` was assigned to.
    fn close(&self, thread: usize, read: *mut Node<T>, next: *mut Node<T>) {
        let dequeuer = unsafe { (*next).dequeuer.load(SeqCst) };
        if dequeuer == thread {
            self.deqreq[dequeuer].store(next, Release);
        } else {
            // If the dequeue result belongs to another thread, it needs to be marked.
            let node = self.pointers.mark_ptr(thread, DEQUEUE, self.deqreq[dequeuer].load(SeqCst));
            if node != next && read == self.read.load(SeqCst) {
                exchange_ptr(&self.deqreq[dequeuer], node, next);
            }
        }
        exchange_ptr(&self.read, read, next);
    }

    /// Cancels the dequeue request and ensures there are no pending dequeues in the pipeline.
    fn rollback(&self, thread: usize, old: *mut Node<T>, req: *mut Node<T>) {
        self.deqold[thread].store(old, SeqCst);
        let read = self.read.load(SeqCst);

        // If another thread dequeued a node for this thread or the queue is still empty, return.
        if self.deqreq[thread].load(SeqCst) != req || read == self.write.load(SeqCst) {
            return;
        }

        self.pointers.mark_ptr(thread, READ, read);
        if read != self.read.load(SeqCst) {
            return;
        }

        let next = self.pointers.mark_ptr(thread, NEXT, unsafe { (*read).next.load(SeqCst) });
        if read != self.read.load(SeqCst) {
            return;
        }

        if !self.assign(read, next) {
            exchange_usize(unsafe { &(*next).dequeuer }, usize::max_value(), thread);
        }
        self.close(thread, read, next);
    }

    /// Steps:
    ///
    /// 1. Signal intent to dequeue by storing matching values in `deqreq` and `deqold`.
    /// 2. Set `read.dequeuer` to `thread`
    /// 3. Set `deqreq[thread]` to point to `read`
    /// 4. Advance `read` to point to `read.next`
    ///
    /// Threads will attempt to dequeue nodes for other threads following the steps above until they
    /// have a dequeued node of their own.
    fn consume(&self, thread: usize) -> Result<T, ConsumeError> {
        // Step 1.
        let old = self.deqold[thread].load(SeqCst);
        let req = self.deqreq[thread].load(SeqCst);
        self.deqold[thread].store(req, SeqCst);

        for _ in 0..self.deqreq.len() {
            // If this thread has a dequeued node of its own, break.
            if self.deqreq[thread].load(SeqCst) != req {
                break;
            }

            // Load the current read pointer.
            let read = self.pointers.mark_ptr(thread, READ, self.read.load(SeqCst));
            // If another thread has advanced the read pointer, skip this iteration.
            if read != self.read.load(SeqCst) {
                continue;
            }

            // If the read and write pointers are equal, the queue may be empty.
            if read == self.write.load(SeqCst) {
                // Step 1 rollback.
                self.rollback(thread, old, req);

                // If another thread dequeued a node for this thread, break.
                if self.deqreq[thread].load(SeqCst) != req {
                    self.deqold[thread].store(req, Relaxed);
                    break;
                }

                self.pointers.clear(thread, READ);
                self.pointers.clear(thread, NEXT);
                self.pointers.clear(thread, DEQUEUE);
                if self.producers.load(Acquire) == 0 {
                    return Err(ConsumeError::Disconnected);
                } else {
                    return Err(ConsumeError::Empty);
                }
            }

            // Load the current next pointer.
            let next = self.pointers.mark_ptr(thread, NEXT, unsafe { (*read).next.load(SeqCst) });
            // If another thread has advanced the read pointer, skip this iteration.
            if read != self.read.load(SeqCst) {
                continue;
            }

            // Step 2.
            if self.assign(read, next) {
                // Step 3 and Step 4.
                self.close(thread, read, next);
            }
        }

        let node = self.deqreq[thread].load(SeqCst);

        // Step 4 again, just in case.
        let read = self.pointers.mark_ptr(thread, READ, self.read.load(SeqCst));
        let next = unsafe { (*read).next.load(SeqCst) };
        if read == self.read.load(SeqCst) && node == next {
            exchange_ptr(&self.read, read, next);
        }

        self.pointers.clear(thread, READ);
        self.pointers.clear(thread, NEXT);
        self.pointers.clear(thread, DEQUEUE);
        self.pointers.retire(thread, old);
        Ok(unsafe { (*node).item.take().unwrap() })
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe { BoxMemory.deallocate(self.sentinel); }
        while self.consume(0).is_ok() { }
        for (req, old) in self.deqreq.iter().zip(self.deqold.iter()) {
            unsafe { BoxMemory.deallocate(req.load(Relaxed)); }
            unsafe { BoxMemory.deallocate(old.load(Relaxed)); }
        }
    }
}

unsafe impl<T> Sync for Queue<T> where T: Send { }

//================================================
// Functions
//================================================

fn exchange_ptr<T>(atomic: &AtomicPtr<T>, current: *mut T, new: *mut T) {
    let _ = atomic.compare_exchange(current, new, SeqCst, SeqCst);
}

fn exchange_usize(atomic: &AtomicUsize, current: usize, new: usize) {
    let _ = atomic.compare_exchange(current, new, SeqCst, SeqCst);
}

/// Returns a producer and consumer for an unbounded MPMC wait-free queue.
///
/// The value of `producers` indicates the maximum number of clones allowed of the initial producer.
/// The value of `consumers` indicates the maximum number of clones allowed of the initial consumer.
pub fn channel<T>(producers: usize, consumers: usize) -> (Producer<T>, Consumer<T>) {
    let queue = Queue::new(producers + 1, consumers + 1);
    (Producer(0, queue.clone()), Consumer(0, queue))
}
