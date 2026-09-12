import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';

vi.mock('../producer/streamProducer', () => ({
    startStreamProduction: vi.fn().mockResolvedValue(undefined),
    stopStreamProduction: vi.fn()
}))

import { startStreamProduction, stopStreamProduction } from '../producer/streamProducer';
import app from '../app';

describe('streams routes', () => {
    describe('POST /api/streams', () => {
        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should start stream production and return 200', async () => {
            const res = await request(app)
                .post('/api/streams')
                .send({ prompt: 'hello' })

            expect(res.status).toBe(201)
            expect(res.body.streamId).toBeTypeOf('string')
            expect(startStreamProduction).toHaveBeenCalledWith(res.body.streamId, 'hello')
        });

        it('should return 400 if streamId or prompt is missing', async () => {
            const res = await request(app).post('/api/streams').send({});
            expect(res.status).toBe(400)
            expect(startStreamProduction).not.toHaveBeenCalled()
        });
    });
    describe('DELETE /api/streams/:streamsId', () => {
        beforeEach(() => { vi.clearAllMocks })
        const streamId = 'test-123'

        it('should stop stream production and return', async () => {
            vi.mocked(stopStreamProduction).mockReturnValue(true)

            const res = await request(app)
                .delete(`/api/streams/${streamId}`)

            expect(res.status).toBe(200)

        })
        it('should return error message when streamId not found', async () => {
            vi.mocked(stopStreamProduction).mockReturnValue(false)
            const res = await request(app)
                .delete(`/api/streams/${streamId}`)
            expect(res.status).toBe(404)
        })
    })
})
