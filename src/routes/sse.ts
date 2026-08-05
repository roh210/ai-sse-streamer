import { Router, Request, Response } from 'express';
import { streamTokens } from '../ai/aiProvider';


const router: Router = Router();

const SSE_HEADERS = {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
}

router.get('/streams/:id', async (req: Request, res: Response) => {
    const streamId = req.params.id;
     
    res.writeHead(200, SSE_HEADERS);
    res.flushHeaders(); // Flush the headers to establish SSE with the client
   
    const abortController = new AbortController();
    req.on('close', () => {
        console.log(`[${streamId}] Client disconnected`);
        abortController.abort();
    });

    try {
        for await (const token of streamTokens('Write a 200 word paragraph about the ocean', abortController.signal)) {
            console.log(`[${streamId}] writing token`);
            res.write(`event: token\ndata: ${JSON.stringify({ text: token })}\n\n`);
        }
        res.write(`event: done\ndata: {}\n\n`);
    }
    catch (error) {
        console.error(`[${streamId}] Error streaming tokens:`, error);
        res.write(`event: error\ndata: ${JSON.stringify({ message: 'stream failed' })}\n\n`);
    }

    res.end();
});

export default router;