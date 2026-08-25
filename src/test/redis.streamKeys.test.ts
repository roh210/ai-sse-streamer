import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { cutoffId, isValidStreamKey, streamKey } from '../redis/streamKeys';


describe('redis Stream Keys', () => {
    describe('cutoffId', () => {
        beforeEach(() => {
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2026-08-25T12:00:00.000Z'));
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('returns cutoff id 1 hour before the frozen time', () => {
            const id = cutoffId(1);
            const expected = new Date('2026-08-25T11:00:00.000Z').getTime();
            expect(id).toBe(`${expected}-0`);
        })
    });
    describe('isValidStreamKey', () => {
        it('returns true for valid stream keys', () => {
            expect(isValidStreamKey('validKey123')).toBe(true);
            expect(isValidStreamKey('another_valid-key')).toBe(true);
        });

        it('returns false for invalid stream keys', () => {
            expect(isValidStreamKey('invalid key with spaces')).toBe(false);
            expect(isValidStreamKey('invalid$key')).toBe(false);
            expect(isValidStreamKey('invalid/key')).toBe(false);
        });
    });
    describe('streamKey', () => {
        it('returns the correct stream key for valid ids', () => {
            expect(streamKey('validKey123')).toBe('stream:validKey123');
            expect(streamKey('another_valid-key')).toBe('stream:another_valid-key');
        });

        it('throws an error for invalid ids', () => {
            expect(() => streamKey('invalid key with spaces')).toThrow('Invalid stream key: invalid key with spaces');
            expect(() => streamKey('invalid$key')).toThrow('Invalid stream key: invalid$key');
            expect(() => streamKey('invalid/key')).toThrow('Invalid stream key: invalid/key');
        });
    })
})