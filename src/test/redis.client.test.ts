import {describe, it, expect} from 'vitest'
import { connectRedis, redisClient } from '../redis/client';


describe('redis Client', () =>{
    it('connects and responds to ping', async () => {
        await connectRedis();
        const result = await redisClient.ping();
        expect(result).toBe('PONG');
    })
})