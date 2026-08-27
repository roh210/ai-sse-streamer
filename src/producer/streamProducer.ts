import { streamTokens } from "../ai/aiProvider";
import { redisClient } from "../redis/client"
import { cutoffId, streamCountKey, streamKey } from "../redis/streamKeys"
import { APIUserAbortError } from '@anthropic-ai/sdk';

const controllers = new Map<string, AbortController>();

export const recordToken = async (streamId: string, token: string) => {
    const key = streamKey(streamId);
    const countKey = streamCountKey(streamId);

    const multi = redisClient.multi();
    multi.xAdd(key, '*', { token });
    multi.incr(countKey);
    multi.xTrim(key, 'MINID', cutoffId(24), { strategyModifier: '~' });

    const results = await multi.exec();
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
            await recordToken(streamId, token);
        }
    } catch (error) {
        if (error instanceof APIUserAbortError) {
            return
        }
        throw error
    }
    finally {
        controllers.delete(streamId);
    }
}

export const stopStreamProduction = (streamId: string): void => {
    const controller = controllers.get(streamId);
    controller?.abort();
}