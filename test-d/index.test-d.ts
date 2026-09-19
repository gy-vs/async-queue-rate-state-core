import {expectType} from 'tsd';
import PQueue from '../source/index.js';

const queue = new PQueue();

expectType<Promise<string>>(queue.add(async () => '🦄'));

expectType<boolean>(queue.isRateLimited);

expectType<PQueue>(queue.on('rateLimit', () => queue.isRateLimited));
expectType<PQueue>(queue.on('rateLimitCleared', () => queue.isRateLimited));
