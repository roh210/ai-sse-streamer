import { Router, Request, Response } from 'express';
import { streamTokens } from '../ai/aiProvider';
import { createRingBuffer } from '../buffer/ringBuffer';
import chalk from 'chalk';


const router: Router = Router();

const SSE_HEADERS = {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
}

const buffers = new Map<string, ReturnType<typeof createRingBuffer>>();

router.get('/streams/:id', async (req: Request, res: Response) => {
    console.log(chalk.cyan('[sse] handler entered'))
    const streamId = req.params.id as string;

    if (!buffers.has(streamId)) buffers.set(streamId, createRingBuffer(5));

    const buffer = buffers.get(streamId)!;

    res.writeHead(200, SSE_HEADERS);
    res.flushHeaders(); // Flush the headers to establish SSE with the client

    const lastEventId = req.headers['last-event-id'];

    if (lastEventId !== undefined) {
        const replayEvents = buffer.getFrom(Number(lastEventId));
        if (replayEvents !== null) {
            console.log(chalk.green(`[${streamId}] Replaying ${replayEvents.length} events`));
            for (const entry of replayEvents) {
                res.write(`id: ${entry.id}\nevent: ${entry.event}\ndata: ${entry.data}\n\n`);
            }
            res.end()
            return
        } else {
            console.log(chalk.green(`[${streamId}] No events to replay`));
            res.write(`event: resync\ndata: ${JSON.stringify({ message: 'No events to replay' })}\n\n`);
            res.end()
            return
        }
    }

    const abortController = new AbortController();
    req.on('close', () => {
        console.log(chalk.yellow(`[${streamId}] Client disconnected`));
        abortController.abort();
    });

    try {
        for await (const token of streamTokens('Write a 200 word paragraph about the ocean', abortController.signal)) {
            const event = buffer.push('token', { text: token });
            console.log(chalk.gray(`[${streamId}] writing token`));
            res.write(`id: ${event.id}\nevent: token\ndata: ${JSON.stringify({ text: token })}\n\n`);
        }
        const endEvent = buffer.push('done', {});
        res.write(`id: ${endEvent.id}\nevent: done\ndata: {}\n\n`);
    }
    catch (error) {
        console.error(chalk.red(`[${streamId}] Error streaming tokens:`, error));
        res.write(`event: error\ndata: ${JSON.stringify({ message: 'stream failed' })}\n\n`);
    }

    res.end();
});

export default router;