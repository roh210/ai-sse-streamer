import { Router, Request, Response } from 'express';
import { streamTokens } from '../ai/aiProvider';
import { createRingBuffer } from '../buffer/ringBuffer';
import chalk from 'chalk';
import { addClient, broadcast, closeAll, createGeneration, getGeneration, removeClient } from '../connection/connectionManager';



const router: Router = Router();

const SSE_HEADERS = {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
}

const buffers = new Map<string, ReturnType<typeof createRingBuffer>>();
let openCount  = 0

router.get('/streams/:id', async (req: Request, res: Response) => {
    console.log(chalk.cyan('[sse] handler entered'))

    const streamId = req.params.id as string;
    const abortController = new AbortController();
    const lastEventId = req.headers['last-event-id'];

    if (!buffers.has(streamId)) buffers.set(streamId, createRingBuffer(100));

    const buffer = buffers.get(streamId)!;
    const handleDisconnect = (streamId: string, res: Response) => {
        req.on('close', () => {
         openCount--
         console.log(chalk.dim(`[listeners] open connections: ${openCount}`))
            const controller = removeClient(streamId, res)
            if (controller) {
                console.log(chalk.yellow(`[${streamId}] Client disconnected`))
                controller.abort()
            }
        })
    }


    res.writeHead(200, SSE_HEADERS);
    openCount++
    console.log(chalk.dim(`[open] ${openCount}`))
    res.flushHeaders(); // Flush the headers to establish SSE with the client


    if (lastEventId !== undefined) { // replay for one disconnected client // reconnection stage
        const replayEvents = buffer.getFrom(Number(lastEventId));
        if (replayEvents !== null) {
            console.log(chalk.green(`[${streamId}] Replaying ${replayEvents.length} events`));
            for (const entry of replayEvents) {
                res.write(`id: ${entry.id}\nevent: ${entry.event}\ndata: ${entry.data}\n\n`);
            }

            const gen = getGeneration(streamId)
            if (gen) {
                // generation is still alive -- attach this to it stay open
                gen.clients.add(res)
                handleDisconnect(streamId, res)
            } else {
                // the generation has already finished - that was the whole remaining tail
                res.end()
            }
        } else {
            console.log(chalk.green(`[${streamId}] No events to replay`));
            res.write(`event: resync\ndata: ${JSON.stringify({ message: 'No events to replay' })}\n\n`);
            res.end()
        }
        return
    }

    if (getGeneration(streamId) === undefined) {  // creation stage - new client joins - broad cast
        createGeneration(streamId, res, abortController)
        handleDisconnect(streamId, res)
        try {
            for await (const token of streamTokens('Write a 300 word paragraph about the ocean', abortController.signal)) {
                const event = buffer.push('token', { text: token });
                console.log(chalk.gray(`[${streamId}] writing token`));
                broadcast(streamId, `id: ${event.id}\nevent: token\ndata: ${JSON.stringify({ text: token })}\n\n`);
            }
            const endEvent = buffer.push('done', {});
            broadcast(streamId, `id: ${endEvent.id}\nevent: done\ndata: {}\n\n`);
            closeAll(streamId)
        }
        catch (error) {
            if (abortController.signal.aborted) {
                // last client already left - removeClient already deleted
                // the generation and there's nobody to notify or close
                console.log(chalk.yellow(`[${streamId}] Generation aborted, no clients remaining`))
            } else {
                console.error(chalk.red(`[${streamId}] Error streaming`, error))
                broadcast(streamId, `event: error\ndata: ${JSON.stringify({ message: 'stream failed' })}\n\n`);
                closeAll(streamId)
            }
            
        }
     return 
    } else {   // joining client to an existing stream id - one client only
        addClient(streamId, res)
        handleDisconnect(streamId, res)
        const history = buffer.getAll() //replay whatever is in the buffer for one specific client
        const truncated = history.length > 0 && history[0].id !== 0 
        if(truncated){
            res.write(`event: partial\ndata: ${JSON.stringify({message:'Some earlier history was evicted'})}\n\n`)
        }
        console.log(chalk.green(`[${streamId}] Replaying ${history.length} events`));
        for (const entry of history) {
            res.write(`id: ${entry.id}\nevent: ${entry.event}\ndata: ${entry.data}\n\n`);
        }

        return
    }

    res.end();
});

export default router;