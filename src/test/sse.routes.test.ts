import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';


vi.mock('../consumer/resumeHandler', () => ({ resolveCursor: vi.fn() }));
vi.mock('../connection/connectionManager', () => ({ register: vi.fn(), deregister: vi.fn() }));

import { resolveCursor } from '../consumer/resumeHandler';
import { register, deregister } from '../connection/connectionManager';
import app from '../app';

describe('GET /api/streams/:id', () => {
    beforeEach(() => vi.clearAllMocks());

    it('writes a resync event and closes when the cursor is stale', async () => {
        vi.mocked(resolveCursor).mockResolvedValue({ type: 'resync' });

        const response = await request(app).get('/api/streams/test-stream');

        expect(response.text).toContain('event: resync');
        expect(register).toHaveBeenCalledWith('test-stream', expect.anything());
        expect(deregister).toHaveBeenCalledWith('test-stream', expect.anything());
    });
});
