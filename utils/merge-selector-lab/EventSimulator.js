import Heap from 'https://cdn.skypack.dev/heap-js';

export class Event
{
    constructor(name, callback, dependencies = [])
    {
        this.stopped = false;
        this.time = 0;
        this.name = name;
        this.callback = callback;
        this.dependencies = dependencies; // List of events this event depends on
        this.executed = false;  // Whether the event has been executed
        this.dependents = [];   // List of events that depend on this event
        this.dependencies_left = 0; // Count of unsatisfied dependencies
        for (const dep of dependencies)
        {
            if (!dep.executed)
                this.dependencies_left++;
        }
    }

    // Add a dependent event
    addDependent(event)
    {
        this.dependents.push(event);
    }

    // Resolve a dependency
    resolveDependency(sim)
    {
        this.dependencies_left--;
        if (this.dependencies_left === 0)
            sim._dependenciesResolved(this);
    }

    // Execute the event
    async execute(sim)
    {
        // console.log("EXEC", sim.time, this.name, this);
        await this.callback(sim, this);
        this.executed = true;

        // Once this event is executed, trigger its dependents
        for (const dependent of this.dependents)
            dependent.resolveDependency(sim);
    }
}

export class EventSimulator
{
    constructor()
    {
        this.ready_events = []; // Events ready to be executed
        this.pending_events = new Heap((a, b) => a.time - b.time); // Use min Heap for pending events
        this.time = 0;
        this.timers = [];
        this.last_timer = 0;
        this.min_timer_interval_ms = 100; // At least check every 100 ms
    }

    // Schedule new event to be executed at specific time
    scheduleAt(time, name, callback, dependencies = [])
    {
        const event = new Event(name, callback, dependencies);
        this.scheduleEventAt(time, event);
        return event;
    }

    // Schedule given event to be executed at specific time
    scheduleEventAt(time, event)
    {
        if (isNaN(time)) // Prevent hangups
            throw { message: "Scheduling event to NaN time:", event};

        event.time = time;
        // Classify the event based on its time and dependencies
        if (event.dependencies.length === 0)
            this._dependenciesResolved(event);
        else
            // Link this event's dependencies to track dependents
            event.dependencies.forEach(dep => dep.addDependent(event));
    }

    // Schedule event to be executed after all current ready events
    postpone(name, callback)
    {
        this.scheduleAt(0, name, callback, []);
    }

    // Register a callback to be called every `interval` ms in realtime (not simulation)
    addTimer(interval_ms, callback, adjustable = true)
    {
        this.timers.push({
            last: performance.now(),
            interval_ms,
            callback,
            adjustable,
            calls: 0,
            real_interval_ms: interval_ms,
            avg_exec_ms: 0,
        });
        this.min_timer_interval_ms = Math.min(this.min_timer_interval_ms, interval_ms);
    }

    // Handle timers
    async #handleTimers()
    {
        if (this.timers.length > 0)
        {
            const now = performance.now();
            if (this.last_timer + this.min_timer_interval_ms < now)
            {
                this.last_timer = now;
                for (const timer of this.timers)
                {
                    if (timer.last + timer.real_interval_ms < now)
                    {
                        timer.last = now;
                        timer.calls++;
                        if (timer.started == undefined)
                            timer.started = now;
                        const before_ms = performance.now();
                        await timer.callback();
                        const after_ms = performance.now();
                        const exec_ms = after_ms - before_ms;
                        timer.avg_exec_ms = 0.9 * timer.avg_exec_ms + 0.1 * exec_ms;
                        if (timer.adjustable)
                            // We dont want a timer to consume more than 10% of time
                            timer.real_interval_ms = Math.max(timer.real_interval_ms, timer.avg_exec_ms * 10);
                    }
                }
            }
        }
    }

    // Run the simulation
    async run()
    {
        const startTime = performance.now();
        while (!this.stopped && (this.ready_events.length > 0 || this.pending_events.size() > 0))
        {
            await this.#executeNextEvent();
            await this.#handleTimers();
        }
    }

    stop()
    {
        this.stopped = true;
    }

    // All dependencies of an event are now satisfied
    _dependenciesResolved(event)
    {
        if (event.time <= this.time) // Event is ready to be executed
            this.ready_events.push(event);
        else // Time is in the future, event is pending
            this.pending_events.push(event);
    }

    // Execute the next available event
    async #executeNextEvent()
    {
        if (this.ready_events.length > 0)
        {
            const event = this.ready_events.shift(); // Take the first event from ready_events
            await event.execute(this); // Execute the event
        }
        else if (this.pending_events.size() > 0)
        {
            // Move time forward to the next pending event's time
            this.time = this.pending_events.peek().time;
            // Move pending events to ready events if their time has arrived
            while (this.pending_events.size() > 0 && this.pending_events.peek().time <= this.time)
                this.ready_events.push(this.pending_events.pop()); // Move event to ready_events
        }
    }
}
