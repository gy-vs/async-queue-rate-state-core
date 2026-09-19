import {test} from 'node:test';
import assert from 'node:assert/strict';
import delay from 'delay';
import PQueue from '../source/index.js';

// Flush microtasks so queued `#next` callbacks and task continuations settle.
const flushMicrotasks = async () => {
	for (let index = 0; index < 10; index++) {
		// eslint-disable-next-line no-await-in-loop
		await Promise.resolve();
	}
};

const enableMockTimers = (t: Parameters<Parameters<typeof test>[1]>[0]) => {
	t.mock.timers.enable({apis: ['Date', 'setTimeout', 'setInterval'], now: 1000});
};

test('isRateLimited is false when the interval quota is exhausted but nothing is waiting', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	// The single running task consumes the whole quota, but nothing is waiting.
	await queue.add(() => '🦄');

	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 0);
	assert.equal(clearedCount, 0);

	// The window expiring with an empty queue must not produce events either.
	t.mock.timers.tick(1000);
	await flushMicrotasks();

	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 0);
	assert.equal(clearedCount, 0);
});

test('rateLimit and rateLimitCleared fire once around the window boundary', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const first = queue.add(() => ran.push('first'));
	const second = queue.add(() => ran.push('second'));

	// The second task is waiting on the interval quota, not on a concurrency slot.
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 0);

	// Just before the window resets, the task is still waiting.
	t.mock.timers.tick(99);
	await flushMicrotasks();
	assert.deepEqual(ran, ['first']);
	assert.equal(queue.isRateLimited, true);
	assert.equal(clearedCount, 0);

	// Crossing the boundary lets the waiting task continue and clears the limit.
	t.mock.timers.tick(1);
	await Promise.all([first, second]);
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('multiple waiting tasks share a single continuous rate-limit period', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);
	const jobs = [
		queue.add(task('first')),
		queue.add(task('second')),
		queue.add(task('third')),
		queue.add(task('fourth')),
	];

	assert.equal(rateLimitCount, 1);

	// Each window runs exactly one more task; the queue stays rate limited
	// throughout, without re-firing `rateLimit`.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 0);

	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.deepEqual(ran, ['first', 'second', 'third']);
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 0);

	t.mock.timers.tick(100);
	await Promise.all(jobs);
	assert.equal(ran.length, 4);
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);

	// No stray timers keep firing after the queue has drained.
	t.mock.timers.tick(10_000);
	await flushMicrotasks();
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('waiting on a concurrency slot does not trigger rateLimit', async t => {
	enableMockTimers(t);

	const queue = new PQueue({concurrency: 1, interval: 100, intervalCap: 10});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const jobs = [
		queue.add(() => 'first'),
		queue.add(() => 'second'),
		queue.add(() => 'third'),
	];

	// Two tasks are queued, but the interval quota is not exhausted.
	assert.equal(queue.size, 2);
	assert.equal(queue.isRateLimited, false);

	await Promise.all(jobs);
	assert.equal(rateLimitCount, 0);
	assert.equal(clearedCount, 0);
});

test('rateLimit applies when the interval quota is exhausted even if concurrency is also saturated', async t => {
	enableMockTimers(t);

	const queue = new PQueue({concurrency: 1, interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);
	const jobs = [queue.add(task('first')), queue.add(task('second'))];

	// The second task cannot run even once the slot frees, because the quota is gone.
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);

	t.mock.timers.tick(100);
	await Promise.all(jobs);
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('rateLimitCleared fires when the quota recovers while paused', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);
	const first = queue.add(task('first'));
	const second = queue.add(task('second'));

	assert.equal(rateLimitCount, 1);

	queue.pause();

	// The window resets while paused: the quota is available again even though
	// the paused queue cannot run the task yet.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.equal(queue.isRateLimited, false);
	assert.equal(clearedCount, 1);
	assert.deepEqual(ran, ['first']);

	// Resuming runs the task without another rate-limit cycle.
	queue.start();
	await Promise.all([first, second]);
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);

	// The interval timer does not linger after the queue drained.
	t.mock.timers.tick(10_000);
	await flushMicrotasks();
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('a queue paused before any run is not rate limited', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1, autoStart: false});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);
	const first = queue.add(task('first'));
	const second = queue.add(task('second'));

	// Nothing has consumed the quota yet; the tasks are waiting on `start()`.
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 0);

	queue.start();
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);

	t.mock.timers.tick(100);
	await Promise.all([first, second]);
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('clear() while rate limited emits rateLimitCleared and keeps pairing', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);
	const first = queue.add(task('first'));
	void queue.add(task('second')); // Never settles: dropped by clear()

	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);

	queue.clear();

	// With nothing waiting, the queue is no longer rate limited.
	assert.equal(queue.isRateLimited, false);
	assert.equal(clearedCount, 1);

	t.mock.timers.tick(1000);
	await first;
	await flushMicrotasks();
	assert.deepEqual(ran, ['first']);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('emptying and re-enqueuing keeps rate-limit events paired', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);

	await queue.add(task('first'));
	assert.equal(queue.isRateLimited, false);

	// Re-enqueue within the spacing window: the task waits for the window.
	t.mock.timers.tick(50);
	const second = queue.add(task('second'));
	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);

	t.mock.timers.tick(50);
	await second;
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);

	// Re-enqueue after the window fully expired: runs immediately, no events.
	t.mock.timers.tick(500);
	await queue.add(task('third'));
	assert.deepEqual(ran, ['first', 'second', 'third']);
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('carryoverConcurrencyCount keeps the rate limit until pending tasks drain', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 2, carryoverConcurrencyCount: true});

	let rateLimitCount = 0;
	let clearedCount = 0;
	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => clearedCount++);

	const ran: string[] = [];
	const slow = async (name: string) => {
		await delay(150);
		ran.push(name);
	};

	const first = queue.add(async () => slow('first'));
	const second = queue.add(async () => slow('second'));
	const third = queue.add(() => ran.push('third'));

	assert.equal(queue.isRateLimited, true);
	assert.equal(rateLimitCount, 1);

	// The window resets, but both slots carry over to the still-running tasks.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.deepEqual(ran, []);
	assert.equal(queue.isRateLimited, true);
	assert.equal(clearedCount, 0);

	// The long tasks finish, but the count only resets on the next window.
	t.mock.timers.tick(50);
	await flushMicrotasks();
	assert.deepEqual(ran, ['first', 'second']);
	assert.equal(queue.isRateLimited, true);
	assert.equal(clearedCount, 0);

	t.mock.timers.tick(50);
	await Promise.all([first, second, third]);
	assert.deepEqual(ran, ['first', 'second', 'third']);
	assert.equal(queue.isRateLimited, false);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('adding tasks from a rateLimit listener does not duplicate notifications', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);

	let addedFromListener = false;
	queue.on('rateLimit', () => {
		rateLimitCount++;

		if (!addedFromListener) {
			addedFromListener = true;
			void queue.add(task('extra'));
		}
	});
	queue.on('rateLimitCleared', () => clearedCount++);

	const jobs = [queue.add(task('first')), queue.add(task('second'))];

	assert.equal(rateLimitCount, 1);
	assert.equal(queue.size, 2);

	// One task per window; the extra task joins the same continuous period.
	t.mock.timers.tick(100);
	await flushMicrotasks();
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 0);

	t.mock.timers.tick(100);
	await Promise.all(jobs);
	await flushMicrotasks();
	assert.deepEqual(ran, ['first', 'second', 'extra']);
	assert.equal(rateLimitCount, 1);
	assert.equal(clearedCount, 1);
});

test('adding tasks from a rateLimitCleared listener starts a new paired period', async t => {
	enableMockTimers(t);

	const queue = new PQueue({interval: 100, intervalCap: 1});

	let rateLimitCount = 0;
	let clearedCount = 0;
	const ran: string[] = [];
	const task = (name: string) => () => ran.push(name);

	queue.on('rateLimit', () => rateLimitCount++);
	queue.on('rateLimitCleared', () => {
		clearedCount++;

		if (clearedCount === 1) {
			// The quota for the new window is already consumed, so this re-limits.
			void queue.add(task('followUp'));
		}
	});

	const jobs = [queue.add(task('first')), queue.add(task('second'))];

	t.mock.timers.tick(100);
	await Promise.all(jobs);
	t.mock.timers.tick(100);
	await flushMicrotasks();

	// The follow-up task re-limited the queue and then cleared once it could
	// run: a second, separate pair of events, not duplicates of the first.
	assert.deepEqual(ran, ['first', 'second', 'followUp']);
	assert.equal(rateLimitCount, 2);
	assert.equal(clearedCount, 2);
});
