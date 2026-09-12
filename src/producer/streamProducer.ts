import { streamTokens } from "../ai/aiProvider";
import { redisClient } from "../redis/client"
import { cutoffId, streamCountKey, streamKey } from "../redis/streamKeys"
import { APIUserAbortError } from '@anthropic-ai/sdk';

const controllers = new Map<string, AbortController>();

export const recordToken = async (streamId: string, token: string): Promise<void> => {
    const key = streamKey(streamId);
    const countKey = streamCountKey(streamId);

    const multi = redisClient.multi();
    multi.xAdd(key, '*', { token });
    multi.incr(countKey);
    multi.xTrim(key, 'MINID', cutoffId(24), { strategyModifier: '~' });

    const results = await multi.exec();
     console.log('[recordToken] key:', key, 'raw results:', results);  
    const failed = results?.find((result) => result instanceof Error);
    if (failed) {
        throw new Error(`Failed to record token for stream ${streamId}: ${failed.message}`);
    }

}

export const startStreamProduction = async (streamId: string, prompt: string): Promise<void> => {
    if (controllers.has(streamId)) {
        throw new Error(`Stream production already in progress for streamId: ${streamId}`);
    }

    const controller = new AbortController();
    controllers.set(streamId, controller);

    try {
        for await (const token of streamTokens(prompt, controller.signal)) {
              console.log('[startStreamProduction] got token:', token);
            await recordToken(streamId, token);
        }
        console.log('[startStreamProduction] loop exited normally');
        await markStreamDone(streamId, 'done');
    } catch (error) {
        if (error instanceof APIUserAbortError) {
            await markStreamCancelled(streamId);
            return
        }
        await markStreamDone(streamId, 'error');
        throw error
    }
    finally {
        controllers.delete(streamId);
    }
}

export const stopStreamProduction = (streamId: string): boolean => {
    const controller = controllers.get(streamId);
    if (!controller) return false

    controller.abort();
    return true
}


const markStreamDone = async (streamId: string, event: 'done' | 'error'): Promise<void> => {
   const fields: Record<string, string> =
   event  === 'error'
   ? {event, message: 'Stream production encountered an error'}
   : {event}
    await redisClient.xAdd(streamKey(streamId), '*', fields);
}

const markStreamCancelled = async (streamId: string): Promise<void> => {
    const count = await redisClient.get(streamCountKey(streamId));
    await redisClient.xAdd(streamKey(streamId), '*', { event: 'cancelled', final_token_count: count ?? '0' });
}