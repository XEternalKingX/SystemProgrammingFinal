# Concurrent Task Dispatcher in Rust - Final Project

## Project Overview
This project is a concurrent task dispatcher written in Rust. It simulates a small operating-system-style scheduler where tasks arrive over time, wait in queues, get dispatched to a fixed worker pool, and then complete execution.
The program focuses on concurrency, scheduling, queues, worker pools, shared state, and performance metrics.

## How to Build and Run
Make sure Rust and Cargo are installed.
From the project folder (final-project), run:
```
cargo build
```
then run:
```
cargo run
```

## Summary of Project
The project
generates 1000 tasks automatically,
uses a fixed random seed for repeatable results,
creates both CPU-bound and IO-bound tasks,
simulates task arrivals over time,
places tasks into queues before execution,
uses 8 worker threads,
dispatches tasks using scheduling policies,
records performance metrics,
shuts down cleanly after all tasks finish.

## Experiments
### Experiment A: Balanced Workload
Configuration:

1000 total tasks,
70% IO tasks,
30% CPU tasks,
FIFO policy,
8 workers.

The purpose of this experiment shows how the system behaves with a fairly mixed workload.

### Experiment B: Stressed Workload
Configuration:

1000 total tasks,
80% IO tasks,
20% CPU tasks,
8 workers,
FIFO policy compared with Optimized policy.

The purpose of this experiment shows how the scheduler behaves under a workload where one task type dominates the system.

## Notes
The program may take around one minute to finish because it runs multiple experiments with 1000 tasks each.

## Tool Use Disclosure
I used OpenAI/ChatGPT to help explain Rust concurrency concepts, and help organize documentation correctly.
One example of advice I accepted was using channels for communication between the generator, dispatcher, and workers.
One example of advice I rejected was complex layouts, as I already was working on a base.


