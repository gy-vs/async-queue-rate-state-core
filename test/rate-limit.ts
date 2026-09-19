import {test, type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import pDefer from 'p-defer';
import PQueue from '../source/index.js';

// Enable mock timers starting at a non-zero timestamp. Starting at 0 would hit
// an edge case in the queue where a zero `intervalEnd` looks like a pending
// interval and the first task would not start immediately.
const enableMockTimers = (t: TestContext): void => {
	t.mock.timers.enable({apis: ['setTimeout', 'setInterval', 'Date'], now: 10_000});
};

// Flush microtasks so task completions and the `queueMicrotask`-scheduled
// `#next()` callbacks run before asserting on state.
const flushMicrotasks = async () => {
	await new Promise(resolve => {
		setImmediate(resolve);
	});
};

const trackRateLimitEvents = (queue: PQueue): string[] => {
	const events: string[] = [];
	queue.on('rateLimit', () => {
		events.push('rateLimit');
	});
	queue.on('rateLimitCleared', () => {
		events.push('rateLimitCleared');
	});
	return events;
};

test('isRateLimited and rateLimit events across interval window boundaries', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	assert.equal(queue.isRateLimited, false);

	const first = queue.add(async () => 'first');
	assert.equal(queue.isRateLimited, false);

	const second = queue.add(async () => 'second');
	// The window's quota is exhausted and a task is waiting for it
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Just before the window boundary: nothing changes
	t.mock.timers.tick(999);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Crossing the window boundary releases the waiting task
	t.mock.timers.tick(1);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await first;
	await second;
	await queue.onIdle();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);
});

test('isRateLimited is false when quota is exhausted but no tasks are waiting', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	await queue.add(async () => 'task');
	await flushMicrotasks();

	// The window's quota is used up, but nothing is queued
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, []);

	t.mock.timers.tick(2000);
	await queue.onIdle();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, []);
});

test('multiple waiting tasks emit a single rateLimit per continuous limited period', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1, concurrency: 10});
	const events = trackRateLimitEvents(queue);

	const tasks = [
		queue.add(async () => 1),
		queue.add(async () => 2),
		queue.add(async () => 3),
		queue.add(async () => 4),
	];

	// One task is running, three are waiting on the interval cap
	assert.equal(queue.pending, 1);
	assert.equal(queue.size, 3);
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Each window releases exactly one task; the queue stays limited until the last one starts
	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await Promise.all(tasks);
	await queue.onIdle();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);
});

test('concurrency saturation does not trigger rateLimit', async t => {
	enableMockTimers(t);

	const queue = new PQueue({concurrency: 1, interval: 1000, intervalCap: 5});
	const events = trackRateLimitEvents(queue);

	const {promise: firstPromise, resolve: resolveFirst} = pDefer<string>();
	const first = queue.add(() => firstPromise);
	const second = queue.add(async () => 'second');
	const third = queue.add(async () => 'third');

	// Tasks are waiting for a concurrency slot, not for interval quota
	assert.equal(queue.pending, 1);
	assert.equal(queue.size, 2);
	assert.equal(queue.isRateLimited, false);

	resolveFirst('first');
	await first;
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);

	t.mock.timers.tick(5000);
	await Promise.all([second, third]);
	await queue.onIdle();
	assert.deepEqual(events, []);
});

test('waiting for a concurrency slot becomes rate limited only once quota is the blocker', async t => {
	enableMockTimers(t);

	const queue = new PQueue({concurrency: 1, interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	const {promise: firstPromise, resolve: resolveFirst} = pDefer<string>();
	const first = queue.add(() => firstPromise);
	const second = queue.add(async () => 'second');

	// Both limits are reached, but the running task holds the only concurrency
	// slot, so the queued task is not considered rate limited yet
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, []);

	// Free the concurrency slot while quota is still exhausted
	resolveFirst('first');
	await first;
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// The window rolls over and the waiting task runs
	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await second;
	await queue.onIdle();
});

test('isRateLimited is always false without an interval configuration', async () => {
	const queue = new PQueue({concurrency: 1});
	const events = trackRateLimitEvents(queue);

	const {promise, resolve} = pDefer<string>();
	const first = queue.add(() => promise);
	const second = queue.add(async () => 'second');

	assert.equal(queue.size, 1);
	assert.equal(queue.isRateLimited, false);

	resolve('first');
	await Promise.all([first, second]);
	await queue.onIdle();
	assert.deepEqual(events, []);
});

test('quota recovery while paused emits rateLimitCleared and stays paired', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	const first = queue.add(async () => 'first');
	const second = queue.add(async () => 'second');
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	queue.pause();

	// Quota recovers at the window boundary even while paused
	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isPaused, true);
	assert.equal(queue.isRateLimited, false);
	assert.equal(queue.size, 1);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	// Resuming runs the waiting task without further rate-limit events
	queue.start();
	await flushMicrotasks();
	assert.equal(queue.size, 0);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await first;
	await second;
	await queue.onIdle();
});

test('rate limit state is tracked while paused', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	await queue.add(async () => 'first');
	await flushMicrotasks();

	queue.pause();

	// The window's quota is already exhausted by the first task
	const second = queue.add(async () => 'second');
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// While paused, no interval timer is armed, so the state is frozen even
	// though the window boundary passes
	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Resuming re-arms the interval machinery, which recovers the quota
	queue.start();
	t.mock.timers.tick(1);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await second;
	await queue.onIdle();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);
});

test('clear() while rate limited emits rateLimitCleared and keeps events paired', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	const first = queue.add(async () => 'first');
	queue.add(async () => 'second');
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	queue.clear();
	assert.equal(queue.size, 0);
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	// The window boundary passes quietly: no further events
	t.mock.timers.tick(2000);
	await flushMicrotasks();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await first;
	await queue.onIdle();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);
});

test('adding tasks while rate limited does not emit duplicate rateLimit events', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	// Adding from within the listener must not cause duplicate notifications
	queue.on('rateLimit', () => {
		queue.add(async () => 'from-listener');
	});

	const tasks = [queue.add(async () => 0)];
	for (let index = 1; index <= 3; index++) {
		tasks.push(queue.add(async () => index));
	}

	assert.equal(queue.size, 4);
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Drain one task per window, including the listener-added task
	for (let index = 0; index < 4; index++) {
		t.mock.timers.tick(1000);
		// eslint-disable-next-line no-await-in-loop
		await flushMicrotasks();
	}

	await Promise.all(tasks);
	await queue.onIdle();
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);
});

test('adding a task from an active listener does not emit spurious rateLimit events', async t => {
	enableMockTimers(t);

	const queue = new PQueue({concurrency: 2, interval: 1000, intervalCap: 2});
	const events = trackRateLimitEvents(queue);

	// Add a single extra task exactly while the second task is starting
	let activeEvents = 0;
	queue.on('active', () => {
		activeEvents++;
		if (activeEvents === 2) {
			queue.add(async () => 'from-listener');
		}
	});

	const first = pDefer<string>();
	const second = pDefer<string>();
	const firstTask = queue.add(() => first.promise);
	const secondTask = queue.add(() => second.promise);

	// The queued task is waiting on the concurrency slots held by the two
	// running tasks, so no rate-limit notification is emitted while they start
	assert.equal(queue.size, 1);
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, []);

	// Free one concurrency slot while quota is still exhausted
	first.resolve('first');
	await firstTask;
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	second.resolve('second');
	await secondTask;
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// The window rolls over and the waiting task runs
	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await queue.onIdle();
});

test('rate limit state works when the queue becomes empty and tasks are re-added within the window', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 1});
	const events = trackRateLimitEvents(queue);

	await queue.add(async () => 'first');
	await flushMicrotasks();

	// The queue is idle and the interval timer has been torn down
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, []);

	// Re-add within the same window: the remaining interval is still enforced
	t.mock.timers.tick(400);
	const second = queue.add(async () => 'second');
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// The resume timeout fires at the window boundary and clears the limit
	t.mock.timers.tick(600);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	await second;
	await queue.onIdle();
});

test('rate limit state with carryoverConcurrencyCount', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 1000, intervalCap: 2, carryoverConcurrencyCount: true});
	const events = trackRateLimitEvents(queue);

	const first = pDefer<string>();
	const second = pDefer<string>();

	const firstTask = queue.add(() => first.promise);
	const secondTask = queue.add(() => second.promise);
	const thirdTask = queue.add(async () => 'third');

	// Both running tasks carry over into the next window, so the third task keeps waiting
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);
	assert.deepEqual(events, ['rateLimit']);

	// Finishing one task frees carried-over quota in the next window
	first.resolve('first');
	await firstTask;
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, true);

	t.mock.timers.tick(1000);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.deepEqual(events, ['rateLimit', 'rateLimitCleared']);

	second.resolve('second');
	await Promise.all([secondTask, thirdTask]);
	await queue.onIdle();
});
