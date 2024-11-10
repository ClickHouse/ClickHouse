export class WorkerPool
{
    constructor(sim, size)
    {
        this.size = size; // Number of available workers in the pool
        this.available_workers = size;
        this.queue = []; // Queue of tasks waiting to be processed
        this.sim = sim;
    }

    // Method to schedule a task
    schedule(duration, callback)
    {
        const task = { duration, callback };
        if (this.available_workers > 0)
        {
            // If a worker is available, start the task immediately
            this._startTask(task, this.sim, this);
        }
        else
        {
            // If no worker is available, push the task to the queue
            this.queue.push(task);
            //console.log(`Task with duration ${duration} queued at time ${this.sim.time}`);
        }
    }

    // Method to check the queue and assign tasks to available workers
    _checkQueue()
    {
        while (this.available_workers > 0 && this.queue.length > 0)
            this._startTask(this.queue.shift());
    }

    _startTask(task)
    {
        //console.log(`Starting task with duration ${task.duration} at time ${this.sim.time}`);
        this.available_workers--;
        this.sim.scheduleAt(this.sim.time + task.duration, () =>
        {
            //console.log(`Ending task with duration ${task.duration} at time ${this.sim.time}`);
            task.callback();
            this.available_workers++;
            this._checkQueue();
        });
    }
}
