import { assert } from 'https://cdn.jsdelivr.net/npm/chai/chai.js';

import { EventSimulator } from '../EventSimulator.js';
import { WorkerPool } from '../WorkerPool.js';

// Tests for WorkerPool class
async function testAddFewTasksOneWorker()
{
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 1);

    pool.schedule(5, "test", () => {
        assert.equal(sim.time, 5, "Current time should be 5 when Task 1 completes");
    });
    pool.schedule(3, "test", () => {
        assert.equal(sim.time, 8, "Current time should be 8 when Task 2 completes");
    });
    pool.schedule(2, "test", () => {
        assert.equal(sim.time, 10, "Current time should be 10 when Task 3 completes");
    });

    await sim.run();

    console.log("Test 1 passed");
}

async function testAddSevenTasksThreeWorkers()
{
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 3);

    pool.schedule(4, "test", () => {
        assert.equal(sim.time, 4, "Current time should be 4 when Task A completes");
    });
    pool.schedule(3, "test", () => {
        assert.equal(sim.time, 3, "Current time should be 3 when Task B completes");
    });
    pool.schedule(2, "test", () => {
        assert.equal(sim.time, 2, "Current time should be 2 when Task C completes");
    });
    pool.schedule(1, "test", () => {
        assert.equal(sim.time, 3, "Current time should be 1 when Task D completes");
    });
    pool.schedule(5, "test", () => {
        assert.equal(sim.time, 8, "Current time should be 5 when Task E completes");
    });
    pool.schedule(6, "test", () => {
        assert.equal(sim.time, 9, "Current time should be 6 when Task F completes");
    });
    pool.schedule(7, "test", () => {
        assert.equal(sim.time, 11, "Current time should be 7 when Task G completes");
    });

    await sim.run();

    console.log("Test 2 passed");
}

async function testGenerateTasksUntil100()
{
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 5);
    let taskCount = 0;

    function createTask() {
        if (taskCount < 100) {
            taskCount++;
            pool.schedule(1, "test", () => {
                assert.equal(sim.time, taskCount, `Current time should be ${taskCount} when Task ${taskCount} completes`);
                createTask();
            });
        }
    }
    createTask();

    await sim.run();

    assert.equal(taskCount, 100, "All 100 tasks should be completed");
    console.log("Test 3 passed");
}

async function testGenerateTasksDoubling()
{
    const sim = new EventSimulator();
    const pool = new WorkerPool(sim, 5);
    let taskCount = 0;

    function createTask() {
        if (taskCount < 500) {
            taskCount++;
            pool.schedule(1, "test", () => {
                createTask();
                createTask();
            });
        }
    }
    createTask();

    await sim.run();

    assert.equal(taskCount, 500, "All 500 tasks should be completed");
    assert.isAbove(sim.time, taskCount / pool.size, "Current time should be greater than total task count divided by worker count");
    console.log("Test 4 passed");
}

export async function testWorkerPool()
{
    testAddFewTasksOneWorker();
    testAddSevenTasksThreeWorkers();
    testGenerateTasksUntil100();
    testGenerateTasksDoubling();
}
