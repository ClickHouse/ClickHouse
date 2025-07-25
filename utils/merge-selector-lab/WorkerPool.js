import { Event } from './EventSimulator.js';

export class WorkerPool
{
    constructor(sim, size)
    {
        this.size = size; // Number of available workers in the pool
        this.available_workers = size;
        this.queue = []; // Queue of tasks waiting to be processed
        this.sim = sim;
    }

    isAvailable()
    {
        return this.queue.length < this.available_workers;
    }

    // Method to schedule a task
    schedule(duration, name, callback)
    {
        const task = new Event(name, async (sim, event) => await this.#finishTask(sim, event, callback));
        task.duration = duration;

        if (this.available_workers > 0) // If a worker is available, start the task immediately
            this.#beginTask(task);
        else // If no worker is available, push the task to the queue
            this.queue.push(task);

        return task;
    }

    #beginTask(task)
    {
        //console.log(`Starting task with duration ${task.duration} at time ${this.sim.time}`);
        this.available_workers--;
        this.sim.scheduleEventAt(this.sim.time + task.duration, task);
    }

    async #finishTask(sim, event, callback)
    {
        //console.log(`Ending task with duration ${task.duration} at time ${this.sim.time}`);
        this.available_workers++;
        await callback(sim, event);
        while (this.available_workers > 0 && this.queue.length > 0)
            this.#beginTask(this.queue.shift());
    }

}
