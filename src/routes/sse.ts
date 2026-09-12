import { Router, Request, Response } from 'express';

import chalk from 'chalk';
import { deregister, register } from '../connection/connectionManager';
import { resolveCursor } from '../consumer/resumeHandler';
import { redisClient } from '../redis/client';
import { streamKey } from '../redis/streamKeys';



const router: Router = Router();

const SSE_HEADERS = {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
}
// now we mitigate our buffer 
// register the connection 
// cursor ownership - resolve the cursor - if the cursor is stale - we need to resync the client
//use commands xread and xinfo to get the stream info and check if the cursor is stale
// heart beat comment fail -- means to deregister the connection and close the stream
// we know have a to resolve cursor that will replay history 

let openCount = 0;

router.get('/streams/:id', async (req: Request, res: Response) => {
    console.log(chalk.cyan('[sse] handler entered'))

    const streamId = req.params.id as string;
    const lastEventId = req.headers['last-event-id'] ?? null as string | null;


    res.writeHead(200, SSE_HEADERS);
    openCount++
    console.log(chalk.dim(`[open] ${openCount}`))
    res.flushHeaders(); // Flush the headers to establish SSE with the client

    register(streamId, res);

    let disconnected = false
    let heartbeatTimer: NodeJS.Timeout | undefined;

    heartbeatTimer = setInterval(() => {
        if (disconnected) return
        res.write(`event: heartbeat\ndata: ${JSON.stringify({ message: 'heartbeat' })}\n\n`);
    }, 15000);

    const handleDisconnect = () => {
        if (disconnected) return
        disconnected = true
        openCount--
        console.log(chalk.dim(`[close] ${openCount}`))
        clearInterval(heartbeatTimer);
        deregister(streamId, res);
    }

    res.on('error', () => handleDisconnect())
    req.on('close', () => handleDisconnect())

    // sse relay teritory

    const cursorResult = await resolveCursor(streamId, lastEventId as string | null);

    if (cursorResult.type === 'resync') {
        res.write(`event: resync\ndata: ${JSON.stringify({ message: 'Cursor is stale, please resync' })}\n\n`);
        res.end()
        handleDisconnect()
        return
    }

    let cursorId = cursorResult.id


    while (!disconnected) {
        const result = await redisClient.xRead({ key: streamKey(streamId), id: cursorId }, { BLOCK: 0 });
        const entries = result?.[0]?.messages ?? [];
        if (disconnected) break
        if (!entries || entries.length === 0) continue
        for (const entry of entries) {
            cursorId = entry.id
            const eventType = entry.message.event
            if (eventType === 'done' || eventType === 'error' || eventType === 'cancelled') {
                res.write(`event: ${eventType}\ndata: ${JSON.stringify(entry.message)}\n\n`);
                res.end()
                handleDisconnect()
                return
            }
            res.write(`event: token\nid: ${entry.id}\ndata: ${JSON.stringify({ text: entry.message.token })}\n\n`);
        }
    }

    res.end();
});

export default router;