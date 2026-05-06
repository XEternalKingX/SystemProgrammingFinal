// Concurrent Task Dispatcher, Final Project
// What we need in our program, from handout:
//  generate a large collection of tasks
//  simulate arrival over time
//  place tasks into a queue or queues
//  use a bounded worker pool
//  dispatch tasks according to a scheduling policy
//  record useful statistics
//  shut down cleanly
// You are building a simulation, not a real operating system

// imports random # tool
use rand::rngs::StdRng; 
use rand::{Rng, SeedableRng}; // using StdRng to use a fixed seed so the experiemtns results are repeated
use std::collections::VecDeque; // used for queues because we can push to the back and pop from the front
use std::sync::{mpsc, Arc, Mutex}; // mpsc for channels, Arc and Mutex for shared state between threads
use std::sync::atomic::{AtomicBool, Ordering}; // this is used to safely tell the monitor thread when to stop
use std::thread; // creates threads
use std::time::{Duration, Instant}; // duration for simulated task time and instant for timing metrics

const WORKER_COUNT: usize = 8; // will be using a fixed worker pool size 
const GLOBAL_CPU_LIMIT: u32 = 100; // for simulation, the dispatcher will check tghis befor starting task
const TOTAL_TASKS: usize = 1000; // # of tasks generated
const TASK_DURATION_MS: u64 = 200; //simulates work for amount of timr
const ARRIVAL_INTERVAL_MS: u64 = 20; // to delay between tasks, so it wont arrive all at once
const MONITOR_INTERVAL_MS: u64 = 10; // how often the monitor thread records 

// type of tasks
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskKind {
    Cpu, // uses more
    Io, // uses less
}

// one task
#[derive(Clone, Debug)]
struct Task {
    id: usize, // unique id for task
    arrival_time: Instant, // time when created, used to calculate
    kind: TaskKind, // checking if its cpu or io
    duration: Duration, // how long 
    cpu_cost: u32, // how much cpu
}

// scheduling policy for dispatcher
#[derive(Clone, Copy, Debug)]
enum Policy {
    Fifo, // in arrival order, in and out
    Optimized, // uses separate cpu and io queues
}

// this is to store info after task finishes
#[derive(Clone, Debug)]
struct CompletedTask {
    task: Task, // task thats finished
    start_time: Instant, // when worker runs task
    finish_time: Instant, // when worker finished task
    worker_id: usize, // which worker completed task
}

// messages from dispatcher to workers
#[derive(Debug)]
enum WorkerMessage {
    Run(Task), // tells a worker to run a task
    Shutdown, // tells a worker to stop waiting and exit
}

// for shared metrics that are valuesw that multi. threads need to access
#[derive(Default, Debug)]
struct SharedMetrics {
    current_cpu: u32, // current total being used
    active_workers: usize, // # of workers running tasks
    shared_queue_len: usize, // length of fifo shared queue
    cpu_queue_len: usize, // length of cpu queue
    io_queue_len: usize, // length of io queue
    max_queue_len: usize, // max queue length during run
    samples: Vec<MonitorSample>, // moitir samples
}

// for system activity recorded by monitor thread
#[derive(Clone, Debug)]
struct MonitorSample {
    time_ms: u128, // time since experiment started
    active_workers: usize, //# of active wrokers at the moment
    cpu_usage: u32, // current cpu usage
    total_queue_len: usize, // total # of tasks waiting in queue
}

// this is for the final summary of one experiment, for display
#[derive(Debug)]
struct Summary {
    experiment_name: String,
    policy: Policy,
    total_completed: usize,
    cpu_completed: usize,
    io_completed: usize,
    makespan_ms: u128,
    avg_wait_ms: f64,
    avg_turnaround_ms: f64,
    max_wait_ms: u128,
    avg_worker_utilization: f64,
    avg_cpu_usage: f64,
    max_queue_len: usize,
}

fn main() {
    println!("Concurrent Task Dispatcher\n");

    // experiment A, uses fifo to show baseline behavior
    let balanced = run_simulation("Experiment A: Balanced workload", Policy::Fifo, 700, 300, 42);
    print_summary(&balanced);

    // experiment B-1, use to show fifo behaves under pressure
    let stressed_fifo = run_simulation("Experiment B1: Stressed workload FIFO", Policy::Fifo, 800, 200, 99);
    print_summary(&stressed_fifo);

    // experiemnt B-2, use to show how fifo behaves under pressure but with optimized scheduler
    let stressed_optimized = run_simulation("Experiment B2: Stressed workload Optimized", Policy::Optimized, 800, 200, 99);
    print_summary(&stressed_optimized);

    // comparing between fifo and stressed optimized
    println!("\nComparison:");
    println!(
        "FIFO stressed makespan: {} ms, optimized stressed makespan: {} ms",
        stressed_fifo.makespan_ms, stressed_optimized.makespan_ms
    );
    println!(
        "FIFO stressed avg wait: {:.2} ms, optimized stressed avg wait: {:.2} ms",
        stressed_fifo.avg_wait_ms, stressed_optimized.avg_wait_ms
    );
}

// this runs one experiemnt, parameters control the experiment name, scheduling policy, io and cpu ratio and random seed
fn run_simulation(
    experiment_name: &str,
    policy: Policy,
    io_ratio: u32,
    cpu_ratio: u32,
    seed: u64,
) -> Summary { // this is to record when the simulation starts
    let simulation_start = Instant::now(); // makespan and monitor timestamps
    let shared_metrics = Arc::new(Mutex::new(SharedMetrics::default())); // Arc for multi. threadds to share and Mutex to make sure one thread changes at a time
    let monitor_running = Arc::new(AtomicBool::new(true)); // tells the monitor when to stop

    let (task_tx, task_rx) = mpsc::channel::<Task>(); // channel for generated tasks going to dispatcher
    let (ready_tx, ready_rx) = mpsc::channel::<usize>(); // channel for workers ready
    let (complete_tx, complete_rx) = mpsc::channel::<CompletedTask>(); // channel for workers completed

    let mut worker_senders = Vec::new(); // stores sender channels for each worker
    let mut worker_handles = Vec::new(); // stores worker thread handles to join later

    // creating a fixed size worker pool
    for worker_id in 0..WORKER_COUNT {
        let (worker_tx, worker_rx) = mpsc::channel::<WorkerMessage>(); // eaxh worker gets own channel
        worker_senders.push(worker_tx); // sends worker message values 

        // cloning chennls so workers thread can be used
        let ready_tx_clone = ready_tx.clone();
        let complete_tx_clone = complete_tx.clone();
        let metrics_clone = Arc::clone(&shared_metrics);

        let handle = thread::spawn(move || { // one worker thread
            ready_tx_clone.send(worker_id).unwrap(); // worker will tell dispatcher its available

            // worker will wait for messages until shutdown
            while let Ok(message) = worker_rx.recv() {
                match message {
                    WorkerMessage::Run(task) => {
                        let start_time = Instant::now(); // records when task starts

                        // This sleep simulates the task doing work
                        thread::sleep(task.duration);

                        let finish_time = Instant::now(); // finish time

                        // When the task finishes, free its cpu usage
                        // mark this worker as no longer active
                        {
                            let mut metrics = metrics_clone.lock().unwrap();
                            metrics.current_cpu -= task.cpu_cost; // free cpu
                            metrics.active_workers -= 1; // no longer active
                        }

                        // this will send complete info to dispatcher
                        complete_tx_clone
                            .send(CompletedTask {
                                task,
                                start_time,
                                finish_time,
                                worker_id,
                            })
                            .unwrap();

                            // tells dispatcher this worker is ready
                        ready_tx_clone.send(worker_id).unwrap();
                    }
                    WorkerMessage::Shutdown => break, // shutdown message
                }
            }
        });

        worker_handles.push(handle);
    }

    // cloning shared values for monitor thread
    let monitor_metrics = Arc::clone(&shared_metrics);
    let monitor_flag = Arc::clone(&monitor_running);

    // monitor thread will be recording activity
    let monitor_handle = thread::spawn(move || {
        while monitor_flag.load(Ordering::SeqCst) { // collecting
            thread::sleep(Duration::from_millis(MONITOR_INTERVAL_MS));

            // locking shared metrics
            let mut metrics = monitor_metrics.lock().unwrap();
            let queue_len = metrics.shared_queue_len + metrics.cpu_queue_len + metrics.io_queue_len; // total queue lengths
            let current_cpu = metrics.current_cpu;
            let active_workers = metrics.active_workers;

            metrics.max_queue_len = metrics.max_queue_len.max(queue_len); // tracking max queue length seen

            // saving one monitor sample
            metrics.samples.push(MonitorSample {
                time_ms: simulation_start.elapsed().as_millis(),
                active_workers,
                cpu_usage: current_cpu,
                total_queue_len: queue_len,
            });
        }
    });

    // generator thread creating tasks
    let generator_handle = thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(seed); // fixed seed
        let total_ratio = io_ratio + cpu_ratio; // choose cpu vs io based ratio

        for id in 0..TOTAL_TASKS {
            let random_value = rng.gen_range(0..total_ratio); // randomly choose value

            let kind = if random_value < io_ratio {
                TaskKind::Io
            } else {
                TaskKind::Cpu
            };

            let cpu_cost = match kind {
                TaskKind::Io => rng.gen_range(5..=15),
                TaskKind::Cpu => 35,
            };

            // building task
            let task = Task {
                id,
                arrival_time: Instant::now(),
                kind,
                duration: Duration::from_millis(TASK_DURATION_MS),
                cpu_cost,
            };

            // sending task to dispatcher
            task_tx.send(task).unwrap();

            // Tasks do not all arrive at the same time
            // This simulates a stream of incoming work
            thread::sleep(Duration::from_millis(ARRIVAL_INTERVAL_MS));
        }
    });

    let mut shared_queue: VecDeque<Task> = VecDeque::new(); // queue used by fifo policy
    let mut cpu_queue: VecDeque<Task> = VecDeque::new(); // separate queues using optimized policy
    let mut io_queue: VecDeque<Task> = VecDeque::new(); // separate queues using optimized policy
    let mut idle_workers: VecDeque<usize> = VecDeque::new(); // workers that are available
    let mut completed_tasks = Vec::new(); // stores completed task

    let mut generator_done = false; // tracks if generator is finsihed sending tasks
    let mut optimized_turn = 0; // optimized scheduler to choose io more than cpu

    // main dispatcher loop, till all tasks are complete
    while completed_tasks.len() < TOTAL_TASKS {
        while let Ok(worker_id) = ready_rx.try_recv() { // collecting ready worker mesg.
            if !idle_workers.contains(&worker_id) {
                idle_workers.push_back(worker_id);
            }
        }

        // collecting complete tasks from workers
        while let Ok(completed) = complete_rx.try_recv() {
            completed_tasks.push(completed);
        }

        // receiving new tasks from generator
        match task_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(task) => match policy {
                Policy::Fifo => shared_queue.push_back(task), // fifo puts every task into one shared queue
                // optimized separates tasks by type
                Policy::Optimized => match task.kind {
                    TaskKind::Cpu => cpu_queue.push_back(task),
                    TaskKind::Io => io_queue.push_back(task),
                },
            },
            // if disconnected, done
            Err(mpsc::RecvTimeoutError::Disconnected) => generator_done = true,
            // time out = no new tasks arrived
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        // updates queue sizes for monitor
        update_queue_metrics(
            &shared_metrics,
            shared_queue.len(),
            cpu_queue.len(),
            io_queue.len(),
        );

        // assigning task for available workers
        loop {
            let Some(worker_id) = idle_workers.pop_front() else { // getting idle worker
                break;
            };

            // picking next task based on selected policy
            let next_task = match policy {
                Policy::Fifo => pop_fifo_task_if_possible(&mut shared_queue, &shared_metrics),
                Policy::Optimized => pop_optimized_task_if_possible(
                    &mut cpu_queue,
                    &mut io_queue,
                    &shared_metrics,
                    &mut optimized_turn,
                ),
            };

            if let Some(task) = next_task {
                {
                    let mut metrics = shared_metrics.lock().unwrap(); // updating
                    metrics.current_cpu += task.cpu_cost; // reserving cpu for this task
                    metrics.active_workers += 1; // marking worker active
                }

                // sending task to worker
                worker_senders[worker_id]
                    .send(WorkerMessage::Run(task))
                    .unwrap();
            } else {
                // if no task, put worker in idle
                idle_workers.push_front(worker_id);
                break;
            }
        }

        // updating queue sizes
        update_queue_metrics(
            &shared_metrics,
            shared_queue.len(),
            cpu_queue.len(),
            io_queue.len(),
        );

        // safety conditions when everything is domne
        if generator_done
            && shared_queue.is_empty()
            && cpu_queue.is_empty()
            && io_queue.is_empty()
            && completed_tasks.len() == TOTAL_TASKS
        {
            break;
        }
    }

    generator_handle.join().unwrap(); // waiting for generator thread to finish

    // sending shutdown msg. to workers
    for sender in worker_senders {
        let _ = sender.send(WorkerMessage::Shutdown);
    }

    // waiting for all workers to finish
    for handle in worker_handles {
        handle.join().unwrap();
    }

    monitor_running.store(false, Ordering::SeqCst); // telling monitor thread to stop
    monitor_handle.join().unwrap(); // waiting for monitor thread to finish

    // building and returning experiment summary
    build_summary(
        experiment_name,
        policy,
        completed_tasks,
        shared_metrics,
        simulation_start.elapsed(),
    )
}

// this is the fifo scheduler helper
// looks at front task and only runs if cpu limit allows it
fn pop_fifo_task_if_possible(
    queue: &mut VecDeque<Task>,
    metrics: &Arc<Mutex<SharedMetrics>>,
) -> Option<Task> {
    let cpu_now = metrics.lock().unwrap().current_cpu; // reading current cpu usage
    let task = queue.front()?; // looking at the first task in the queue

    // will only remove and return the task if it fits under cpu limit
    if cpu_now + task.cpu_cost <= GLOBAL_CPU_LIMIT {
        queue.pop_front()
    } else {
        None
    }
}

// this is the optimized scheduler helper
// this uses separate cpu and io queue and prefers io 
fn pop_optimized_task_if_possible(
    cpu_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    metrics: &Arc<Mutex<SharedMetrics>>,
    optimized_turn: &mut usize,
) -> Option<Task> {
    let cpu_now = metrics.lock().unwrap().current_cpu; // reading current cpu usage

    // Weighted idea: trying three io picks, then one cpu pick
    // This would help the 800/200 io-heavy workload keep workers busy
    let prefer_io = *optimized_turn % 4 != 3;
    *optimized_turn += 1;

    if prefer_io {
        // trying io first
        if let Some(task) = io_queue.front() {
            if cpu_now + task.cpu_cost <= GLOBAL_CPU_LIMIT {
                return io_queue.pop_front();
            }
        }

        // if io cannot run or queue empty, try cpu
        if let Some(task) = cpu_queue.front() {
            if cpu_now + task.cpu_cost <= GLOBAL_CPU_LIMIT {
                return cpu_queue.pop_front();
            }
        }
    } else {
        // try cpu first
        if let Some(task) = cpu_queue.front() {
            if cpu_now + task.cpu_cost <= GLOBAL_CPU_LIMIT {
                return cpu_queue.pop_front();
            }
        }

        // if cpu cant run or queue empty, try io
        if let Some(task) = io_queue.front() {
            if cpu_now + task.cpu_cost <= GLOBAL_CPU_LIMIT {
                return io_queue.pop_front();
            }
        }
    }

    None // if no task could be scheduled right now
}

// updating queue length info
fn update_queue_metrics(
    shared_metrics: &Arc<Mutex<SharedMetrics>>,
    shared_len: usize,
    cpu_len: usize,
    io_len: usize,
) {
    let mut metrics = shared_metrics.lock().unwrap(); // locking

    // storing current queue sizes
    metrics.shared_queue_len = shared_len;
    metrics.cpu_queue_len = cpu_len;
    metrics.io_queue_len = io_len;

    // updating max queue length if larger
    metrics.max_queue_len = metrics.max_queue_len.max(shared_len + cpu_len + io_len);
}

// building the final summary for experiment
fn build_summary(
    experiment_name: &str,
    policy: Policy,
    completed_tasks: Vec<CompletedTask>,
    shared_metrics: Arc<Mutex<SharedMetrics>>,
    makespan: Duration,
) -> Summary {
    // total # of completed tasks
    let total_completed = completed_tasks.len();

    // counting cpu tasks
    let cpu_completed = completed_tasks
        .iter()
        .filter(|completed| completed.task.kind == TaskKind::Cpu)
        .count();

    let io_completed = total_completed - cpu_completed; // io tasks that was not cpu

    // wait time = start time - arrive time
    let total_wait_ms: u128 = completed_tasks
        .iter()
        .map(|completed| {
            completed
                .start_time
                .duration_since(completed.task.arrival_time)
                .as_millis()
        })
        .sum();

    // turnaround time = finish time - arrive time
    let total_turnaround_ms: u128 = completed_tasks
        .iter()
        .map(|completed| {
            completed
                .finish_time
                .duration_since(completed.task.arrival_time)
                .as_millis()
        })
        .sum();

        // finding the max wait time in completed tasks
    let max_wait_ms = completed_tasks
        .iter()
        .map(|completed| {
            completed
                .start_time
                .duration_since(completed.task.arrival_time)
                .as_millis()
        })
        .max()
        .unwrap_or(0);

    let metrics = shared_metrics.lock().unwrap(); // locking
    let sample_count = metrics.samples.len().max(1) as f64; // avoding division by zero if no samples

    // the average # of active workers
    let avg_active_workers: f64 = metrics
        .samples
        .iter()
        .map(|sample| sample.active_workers as f64)
        .sum::<f64>()
        / sample_count;

        // average cpu usage
    let avg_cpu_usage: f64 = metrics
        .samples
        .iter()
        .map(|sample| sample.cpu_usage as f64)
        .sum::<f64>()
        / sample_count;

    // returning all summary
    Summary {
        experiment_name: experiment_name.to_string(),
        policy,
        total_completed,
        cpu_completed,
        io_completed,
        makespan_ms: makespan.as_millis(),
        avg_wait_ms: total_wait_ms as f64 / total_completed as f64, // avg. wait = total / # of tasks
        avg_turnaround_ms: total_turnaround_ms as f64 / total_completed as f64, // abg. turnaround = total / # of tasks
        max_wait_ms,
        avg_worker_utilization: (avg_active_workers / WORKER_COUNT as f64) * 100.0, // worker working %
        avg_cpu_usage,
        max_queue_len: metrics.max_queue_len,
    }
}

// this prints the final summary for experiemnt
fn print_summary(summary: &Summary) {
    println!("\n== {} ==", summary.experiment_name);

    println!(
        "1000 tasks, {} workers, cap {}%",
        WORKER_COUNT,
        GLOBAL_CPU_LIMIT
    );

    println!("\n-- results --");

    println!(
        "policy                  : {:?}",
        summary.policy
    );

    println!(
        "tasks completed         : {}",
        summary.total_completed
    );

    println!(
        "CPU tasks completed     : {}",
        summary.cpu_completed
    );

    println!(
        "IO tasks completed      : {}",
        summary.io_completed
    );

    println!(
        "makespan                : {} ms",
        summary.makespan_ms
    );

    println!(
        "avg wait time           : {:.2} ms",
        summary.avg_wait_ms
    );

    println!(
        "avg turnaround time     : {:.2} ms",
        summary.avg_turnaround_ms
    );

    println!(
        "max wait time           : {} ms",
        summary.max_wait_ms
    );

    println!(
        "avg worker utilization  : {:.2}%",
        summary.avg_worker_utilization
    );

    println!(
        "avg CPU usage           : {:.2}%",
        summary.avg_cpu_usage
    );

    println!(
        "max queue length        : {}",
        summary.max_queue_len
    );
}







