import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Transport } from '../transport';
import type { ObservabilityEvent } from '../types';

function evt(id: string): ObservabilityEvent {
    return {
        eventId: id,
        timestamp: Date.now(),
        level: 'error',
        sdk: { name: '@smooai/observability', version: '0.1.0', runtime: 'browser' },
    };
}

describe('Transport', () => {
    const fetchMock = vi.fn();
    beforeEach(() => {
        vi.useFakeTimers();
        fetchMock.mockReset();
        (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;
    });
    afterEach(() => {
        vi.useRealTimers();
    });

    it('batches up to maxBatchSize and flushes immediately when full', async () => {
        fetchMock.mockResolvedValue({ ok: true, status: 202 });
        const t = new Transport({ dsn: 'https://example.com/ingest', maxBatchSize: 3, flushIntervalMs: 1000 }, { canBeacon: false });
        t.enqueue(evt('a'));
        t.enqueue(evt('b'));
        expect(fetchMock).not.toHaveBeenCalled();
        t.enqueue(evt('c'));
        // Triggered immediately by max-batch
        await vi.runOnlyPendingTimersAsync();
        await Promise.resolve();
        expect(fetchMock).toHaveBeenCalledOnce();
        const [url, init] = fetchMock.mock.calls[0]!;
        expect(url).toBe('https://example.com/ingest');
        const body = JSON.parse(init.body);
        expect(body.type).toBe('error');
        expect(body.events).toHaveLength(3);
    });

    it('flushes on timer when batch is not yet full', async () => {
        fetchMock.mockResolvedValue({ ok: true });
        const t = new Transport({ dsn: 'https://example.com', maxBatchSize: 10, flushIntervalMs: 500 }, { canBeacon: false });
        t.enqueue(evt('a'));
        expect(fetchMock).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(600);
        expect(fetchMock).toHaveBeenCalledOnce();
    });

    it('drops oldest events when queue overflows', () => {
        const t = new Transport({ dsn: 'https://example.com', maxBatchSize: 100, flushIntervalMs: 1000, maxQueueSize: 2 }, { canBeacon: false });
        t.enqueue(evt('a'));
        t.enqueue(evt('b'));
        t.enqueue(evt('c'));
        expect(t._queueSize()).toBe(2);
    });

    it('flushBeacon uses navigator.sendBeacon when available', () => {
        const beacon = vi.fn().mockReturnValue(true);
        const t = new Transport({ dsn: 'https://example.com', maxBatchSize: 100, flushIntervalMs: 1000 }, { canBeacon: true, beacon });
        t.enqueue(evt('a'));
        t.flushBeacon();
        expect(beacon).toHaveBeenCalledOnce();
        expect(t._queueSize()).toBe(0);
    });

    it('routes the flush through the injected resilient fetcher (not global fetch)', async () => {
        const fetcher = vi.fn().mockResolvedValue({ ok: true, status: 202 });
        const t = new Transport({ dsn: 'https://example.com/ingest', maxBatchSize: 1, flushIntervalMs: 1000 }, { canBeacon: false, fetcher });
        t.enqueue(evt('a'));
        await vi.runOnlyPendingTimersAsync();
        await Promise.resolve();
        expect(fetcher).toHaveBeenCalledOnce();
        // Global fetch must NOT be used when a fetcher is injected.
        expect(fetchMock).not.toHaveBeenCalled();
        const [url, init] = fetcher.mock.calls[0]!;
        expect(url).toBe('https://example.com/ingest');
        expect(init.method).toBe('POST');
        expect(init.keepalive).toBe(true);
        expect(JSON.parse(init.body).events).toHaveLength(1);
    });

    it('re-queues the batch when the resilient fetcher throws (network/non-2xx after retries)', async () => {
        const fetcher = vi.fn().mockRejectedValue(new Error('circuit open'));
        const t = new Transport({ dsn: 'https://example.com', maxBatchSize: 1, flushIntervalMs: 1000 }, { canBeacon: false, fetcher });
        t.enqueue(evt('a'));
        await vi.runOnlyPendingTimersAsync();
        await Promise.resolve();
        expect(fetcher).toHaveBeenCalled();
        // The event survived for the next flush attempt — it was put back on
        // the queue after the throw rather than being dropped.
        expect(t._queueSize()).toBe(1);
    });

    it('re-queues the batch when native fetch resolves a non-2xx response', async () => {
        const fetcher = vi.fn().mockResolvedValue({ ok: false, status: 503 });
        const t = new Transport({ dsn: 'https://example.com', maxBatchSize: 1, flushIntervalMs: 1000 }, { canBeacon: false, fetcher });
        t.enqueue(evt('a'));
        await vi.runOnlyPendingTimersAsync();
        await Promise.resolve();
        expect(fetcher).toHaveBeenCalled();
        expect(t._queueSize()).toBe(1);
    });
});
