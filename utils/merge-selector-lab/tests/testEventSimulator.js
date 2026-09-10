import { assert } from 'https://cdn.jsdelivr.net/npm/chai/chai.js';

import { EventSimulator } from '../EventSimulator.js';

// Test 1: Order of execution of independent events
async function testOrderOfIndependentEvents()
{
    const sim = new EventSimulator();
    const results = [];

    sim.scheduleAt(3, "test", () => results.push('Event 3'));
    sim.scheduleAt(2, "test", () => results.push('Event 2'));
    sim.scheduleAt(1, "test", () => results.push('Event 1'));

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 2', 'Event 3']);
    console.log('Test 1 passed: Order of independent events executed correctly');
}

// Test 2: Chain of dependent events
async function testChainOfDependentEvents()
{
    const sim = new EventSimulator();
    const results = [];

    const event1 = sim.scheduleAt(1, "test", () => results.push('Event 1'));
    const event2 = sim.scheduleAt(2, "test", () => results.push('Event 2'), [event1]);
    const event3 = sim.scheduleAt(3, "test", () => results.push('Event 3'), [event2]);

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 2', 'Event 3']);
    console.log('Test 2 passed: Chain of dependent events executed correctly');
}

// Test 3: Arbitrary in-tree of dependent events
async function testInTreeOfDependentEvents()
{
    const sim = new EventSimulator();
    const results = [];

    const event1 = sim.scheduleAt(1, "test", () => results.push('Event 1'));
    const event2 = sim.scheduleAt(2, "test", () => results.push('Event 2'));
    const event3 = sim.scheduleAt(3, "test", () => results.push('Event 3'), [event1, event2]);

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 2', 'Event 3']);
    console.log('Test 3 passed: In-tree of dependent events executed correctly');
}

// Test 4: Arbitrary out-tree of dependent events
async function testOutTreeOfDependentEvents()
{
    const sim = new EventSimulator();
    const results = [];

    const event1 = sim.scheduleAt(1, "test", () => results.push('Event 1'));
    const event2 = sim.scheduleAt(2, "test", () => results.push('Event 2'), [event1]);
    const event3 = sim.scheduleAt(3, "test", () => results.push('Event 3'), [event1]);

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 2', 'Event 3']);
    console.log('Test 4 passed: Out-tree of dependent events executed correctly');
}

// Test 5: DAG with a Rhombus inside
async function testDAGWithRhombus()
{
    const sim = new EventSimulator();
    const results = [];

    // Create events in a rhombus shape
    const event1 = sim.scheduleAt(1, "test", () => results.push('Event 1'));
    const event2 = sim.scheduleAt(2, "test", () => results.push('Event 2'), [event1]);
    const event3 = sim.scheduleAt(3, "test", () => results.push('Event 3'), [event1]);
    const event4 = sim.scheduleAt(4, "test", () => results.push('Event 4'), [event2, event3]);

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 2', 'Event 3', 'Event 4']);
    console.log('Test 5 passed: DAG with rhombus structure executed correctly');
}

// Test 6: Complex DAG with combined time and dependencies
async function testComplexDAGWithCombinedTimeAndDependencies()
{
    const sim = new EventSimulator();
    const results = [];

    // Create a complex structure with 7 events
    /**
     *
     *        Event 1
     *        /   |
     *    Event 2 |
     *       |    |  Event 3
     *       |    |   /
     *       |    Event 4
     *        \    /
     *        Event 5   Event 6
     *             \    /
     *              Event 7
     */
    const event1 = sim.scheduleAt(1, "test", () => results.push('Event 1'));
    const event2 = sim.scheduleAt(5, "test", () => results.push('Event 2'), [event1]);
    const event3 = sim.scheduleAt(3, "test", () => results.push('Event 3'));
    const event4 = sim.scheduleAt(4, "test", () => results.push('Event 4'), [event1, event3]);
    const event5 = sim.scheduleAt(6, "test", () => results.push('Event 5'), [event2, event4]);
    const event6 = sim.scheduleAt(2, "test", () => results.push('Event 6'));
    const event7 = sim.scheduleAt(7, "test", () => results.push('Event 7'), [event5, event6]);

    await sim.run();

    assert.deepEqual(results, ['Event 1', 'Event 6', 'Event 3', 'Event 4', 'Event 2', 'Event 5', 'Event 7']);
    console.log('Test 6 passed: Complex DAG with combined time and dependencies executed correctly');
}

// Run all the tests
export async function testEventSimulator()
{
    testOrderOfIndependentEvents();
    testChainOfDependentEvents();
    testInTreeOfDependentEvents();
    testOutTreeOfDependentEvents();
    testDAGWithRhombus();
    testComplexDAGWithCombinedTimeAndDependencies();
}
