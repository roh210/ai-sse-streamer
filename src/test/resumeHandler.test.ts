import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../redis/client', () => ({
    redisClient: {
        xInfoStream: vi.fn()
    }
}))

import { redisClient } from '../redis/client';
import { resolveCursor } from '../consumer/resumeHandler';

describe('resolveCursor', () =>{
    const streamId = 'test-resume'

    beforeEach(() =>{
        vi.clearAllMocks()
    })

    it('returns the beginning cursor when no header is present', async () =>{
        const result = await resolveCursor(streamId, null)
        expect(result).toEqual({type:'cursor', id:'0-0'})
        expect(redisClient.xInfoStream).not.toHaveBeenCalled()
    })
    it('returns the requested id as the cursor when it is still valid', async () =>{
        vi.mocked(redisClient.xInfoStream).mockResolvedValue({
            'first-entry' : {id:'100-0', message:{}},
        } as any)

        const result = await resolveCursor(streamId, '200-0')
        expect(result).toEqual({type:'cursor', id:'200-0'})
    })

    it('returns resync when the requested id predates the oldest surviving entry', async () =>{
        vi.mocked(redisClient.xInfoStream).mockResolvedValue({
            'first-entry': {id:'500-0', message: {}}
        } as any)

        const result = await resolveCursor(streamId, '100-0')
        expect(result).toEqual({type:'resync'})
    })
    it('returns resync when the stream is empty', async () =>{
        vi.mocked(redisClient.xInfoStream).mockResolvedValue({
            'first-entry': null
        } as any)

        const result = await resolveCursor(streamId, '100-0')
        expect(result).toEqual({type:'resync'})
    })
})