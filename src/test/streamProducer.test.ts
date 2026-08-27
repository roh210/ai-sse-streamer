// integration test since it requires a running redis instance
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { connectRedis, redisClient } from '../redis/client';
import { streamCountKey, streamKey } from '../redis/streamKeys';
import { recordToken } from '../producer/streamProducer';

vi.mock('../ai/aiProvider', () => ({
    streamTokens: vi.fn()
}))

import { streamTokens } from '../ai/aiProvider';
import { startStreamProduction, stopStreamProduction } from '../producer/streamProducer';
import { APIUserAbortError } from '@anthropic-ai/sdk';

async function* fakeTokens(tokens: string[]) {
    for (const t of tokens) {
        yield t
    }
}
describe('stream Producer', () => {
    describe('recordToken', () => {
        const streamId = 'test-record-token'

        beforeEach(async () => {
            await connectRedis()
            await redisClient.del(streamKey(streamId))
            await redisClient.del(streamCountKey(streamId))
        })

        afterEach(async () => {
            await redisClient.del(streamKey(streamId))
            await redisClient.del(streamCountKey(streamId))
        })

        it('writes the token to the stream and increments the count', async () => {
            await recordToken(streamId, 'hello')

            const entries = await redisClient.xRange(streamKey(streamId), '-', '+')
            expect(entries).toHaveLength(1)
            expect(entries[0].message.token).toBe('hello')

            const count = await redisClient.get(streamCountKey(streamId))
            expect(count).toBe('1')
        })

        it('accumulates count correctly across multiple tokens', async () => {
            await recordToken(streamId, 'one')
            await recordToken(streamId, 'two')

            const entries = await redisClient.xRange(streamKey(streamId), '-', '+')
            expect(entries).toHaveLength(2)

            const count = await redisClient.get(streamCountKey(streamId))
            expect(count).toBe('2')
        })
    })
    describe('startStreamProduction', () => {
        const streamId = 'test-start-production'

        beforeEach(async () => {
            await connectRedis()
            await redisClient.del(streamKey(streamId))
        })
        afterEach(async () => {
            await redisClient.del(streamKey(streamId))
        })
        it('records every token from the AI stream, in order', async () => {
            vi.mocked(streamTokens).mockImplementation(() => fakeTokens(['a', 'b', 'c']))
            await startStreamProduction(streamId, 'irrelevant prompt')

            const entries = await redisClient.xRange(streamKey(streamId), '-', '+')

            expect(entries.map((e) => e.message.token)).toEqual(['a', 'b', 'c'])
        })

        it('throws if production is already in progress for this streamId', async () => {
            vi.mocked(streamTokens).mockImplementation(() => fakeTokens(['a', 'b']))

            const first = startStreamProduction(streamId, 'prompt')
            await expect(startStreamProduction(streamId, 'prompt')).rejects.toThrow(`Stream production already in progress for streamId: ${streamId}`)

            await first
        })
        it('allows starting again after a previous run completed', async () => {
            vi.mocked(streamTokens).mockImplementation(() => fakeTokens(['a']))

            await startStreamProduction(streamId, 'prompt')

            vi.mocked(streamTokens).mockImplementation(() => fakeTokens(['b']))
            await expect(startStreamProduction(streamId, 'prompt')).resolves.toBeUndefined()
        })
    })
    describe('stopStreamProduction', () => {
        const streamId = 'test-stop-production'

        beforeEach(async () => {
            await connectRedis()
            await redisClient.del(streamKey(streamId))
             await redisClient.del(streamCountKey(streamId))
        })
        afterEach(async () => {
            await redisClient.del(streamKey(streamId))
             await redisClient.del(streamCountKey(streamId))
        })

        it('stops the production and does not record further tokens', async () => {
            let resolveToken: (value: string) => void

            const tokenPromise = new Promise<string>((resolve) => {
                resolveToken = resolve
            })

            vi.mocked(streamTokens).mockImplementation(async function* (_prompt, signal) {
                yield 'first'
                await tokenPromise
                if (signal.aborted) {
                    throw new APIUserAbortError
                }
                yield 'second'
            })
            const productionPromise = startStreamProduction(streamId,'prompt')
            await new Promise((resolve) => setTimeout(resolve,100))  
            
            stopStreamProduction(streamId)
            resolveToken!('second')
            await productionPromise

            const entries = await redisClient.xRange(streamKey(streamId), '-','+')
            expect(entries.map((e) => e.message.token)).toEqual(['first'])
        });
    });
})

