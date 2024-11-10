import Heap from 'https://cdn.skypack.dev/heap-js';

class Event
{
    constructor(time, callback, dependencies = [])
    {
        this.time = time;
        this.callback = callback;
        this.dependencies = dependencies; // List of events this event depends on
        this.executed = false;  // Whether the event has been executed
        this.dependents = [];   // List of events that depend on this event
        this.dependencies_left = dependencies.length; // Count of unsatisfied dependencies
        this.queue_drained = false;
    }

    // Add a dependent event
    addDependent(event)
    {
        this.dependents.push(event);
    }

    // Resolve a dependency
    resolveDependency(simulator)
    {
        this.dependencies_left--;
        if (this.dependencies_left === 0)
            simulator._dependenciesResolved(this);
    }

    // Execute the event
    execute(simulator)
    {
        // console.log(`Executing event at time ${this.time}`);
        this.callback(simulator); // Allow callback to add new events during execution
        this.executed = true;

        // Once this event is executed, trigger its dependents
        this.dependents.forEach(dependent => {
            dependent.resolveDependency(simulator);
        });
    }
}

export class EventSimulator
{
    constructor()
    {
        this.ready_events = []; // Events ready to be executed
        this.pending_events = new Heap((a, b) => a.time - b.time); // Use min Heap for pending events
        this.time = 0;
    }

    // Add an event to the simulator and classify it into appropriate queue
    scheduleAt(time, callback, dependencies = [])
    {
        const event = new Event(time, callback, dependencies);
        // Classify the event based on its time and dependencies
        if (dependencies.length === 0)
            this._dependenciesResolved(event);
        else
            // Link this event's dependencies to track dependents
            dependencies.forEach(dep => dep.addDependent(event));
        return event;
    }

    // IMPORTANT: this function should be called from every callback that could indirectly create new events
    // Look for .then(callback) on promises or better avoid using Promise at all
    ping()
    {
        this.queue_drained = false;
    }

    // Run the simulation
    async run()
    {
        while (this.ready_events.length > 0 || this.pending_events.size() > 0)
            await this._executeNextEvent();
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
    async _executeNextEvent()
    {
        if (this.ready_events.length > 0)
        {
            const event = this.ready_events.shift(); // Take the first event from ready_events
            event.execute(this); // Execute the event
            await this._drainMicrotasks();
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

    async _drainMicrotasks()
    {
        this.queue_drained = false;
        while (!this.queue_drained)
        {
            this.queue_drained = true;
            await Promise.resolve(); // Wait for all microtasks to complete
        }
    }
}
