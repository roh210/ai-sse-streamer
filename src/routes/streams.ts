import { Router, Request, Response } from "express";
import { randomUUID } from "node:crypto";
import { startStreamProduction, stopStreamProduction } from "../producer/streamProducer";


const router: Router = Router();

// what we need to record token , start stream production , stop stream production
// the post request is responsible for generating the stream and starting the production of tokens based on the prompt provided in the request body. 
// The delete request is responsible for stopping the stream production for a given streamId.


// fire and forget since create generation should be an immediate process

router.post('/streams', async (req: Request, res: Response) => {
    // Implementation for starting stream production
    const { prompt } = req.body;
    if (!prompt || typeof prompt !== 'string') {
        return res.status(400).json({ error: 'Invalid prompt provided' });
    }
    const streamId = randomUUID();
    console.log(`[POST] starting stream production for ${streamId}`);
    startStreamProduction(streamId, req.body.prompt)
    .then(() => console.log(`[POST] production finished for ${streamId}`))
    .catch((error) => {
        console.error(`Error in stream production for streamId ${streamId}:`, error);
    });
    res.status(201).json({ streamId });
});

router.delete('/streams/:streamId', async (req: Request, res: Response) => {
    // Implementation for stopping stream production
    const streamId = req.params.streamId as string;

    const stopped = stopStreamProduction(streamId);
    if (!stopped) return res.status(404).json({ error: 'Stream not found or already stopped' });
    res.status(200).json({ message: `Stream production stopped for streamId: ${streamId}` });

});

export default router;