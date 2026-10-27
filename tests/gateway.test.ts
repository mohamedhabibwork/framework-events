import { describe, expect, test } from 'bun:test';
import { EXECUTION_CONTEXT, ExecutionContext } from '@framework/core';
import { Hono } from 'hono';
import { websocket } from 'hono/bun';
import { MemoryEventBus } from '../src/bus';
import { createDomainEvent } from '../src/event';
import { WebsocketGateway } from '../src/gateway';

const context = new ExecutionContext('request', 'ws-test', { id: 'server', type: 'system' });
const ACTOR = { id: 'user-42', type: 'user' } as const;

interface Ws {
  socket: WebSocket;
  waitFor: (match: (message: Record<string, unknown>) => boolean) => Promise<Record<string, unknown>>;
  send: (payload: object) => void;
  close: () => void;
}

function connect(port: number, path = '/ws'): Promise<Ws> {
  const socket = new WebSocket(`ws://127.0.0.1:${port}${path}`);
  const queue: Record<string, unknown>[] = [];
  let notify: (() => void) | undefined;
  socket.addEventListener('message', (m) => {
    queue.push(JSON.parse(String(m.data)) as Record<string, unknown>);
    notify?.();
    notify = undefined;
  });
  const waitFor = (match: (message: Record<string, unknown>) => boolean) =>
    new Promise<Record<string, unknown>>((resolve) => {
      const drain = () => {
        const index = queue.findIndex(match);
        if (index >= 0) {
          const matched = queue[index] as Record<string, unknown>;
          queue.splice(0, index + 1);
          resolve(matched);
          return true;
        }
        notify = drain;
        return false;
      };
      if (!drain()) notify = drain;
    });
  return new Promise((resolve) => {
    socket.addEventListener('open', () =>
      resolve({
        socket,
        waitFor,
        send: (payload) => socket.send(JSON.stringify(payload)),
        close: () => socket.close(),
      }),
    );
  });
}

function serve(gateway: WebsocketGateway): { port: number; stop: () => Promise<void> } {
  const app = new Hono().route('/ws', gateway.routes());
  const server = Bun.serve({ port: 0, hostname: '127.0.0.1', fetch: app.fetch, websocket });
  const port = server.port ?? 0;
  return { port, stop: () => server.stop(true) };
}

describe('WebsocketGateway', () => {
  test('authenticates, opens the private user channel and tracks presence', async () => {
    const bus = new MemoryEventBus();
    const gateway = new WebsocketGateway({ bus, authenticate: () => ACTOR, channels: ['reports'], presence: true });
    const { port, stop } = serve(gateway);
    const presenceEvents: string[] = [];
    await bus.subscribe('reports', (event) => {
      if (event.type.startsWith('reports.presence.')) presenceEvents.push(event.type);
    });

    const client = await connect(port);
    try {
      // The private user channel is subscribed automatically on open.
      await client.waitFor((m) => m.type === 'subscribed' && m.channel === 'user.user-42');

      // Allowlisted channel joins broadcast presence.
      client.send({ action: 'subscribe', channel: 'reports' });
      await client.waitFor((m) => m.type === 'subscribed' && m.channel === 'reports');
      expect(gateway.online('reports')).toEqual(['user-42']);
      await Bun.sleep(20);
      // Presence broadcasts are async on the bus; poll briefly for delivery.
      for (let waited = 0; waited < 1000 && presenceEvents.length === 0; waited += 25) {
        await Bun.sleep(25);
      }
      expect(presenceEvents.some((type) => type.endsWith('presence.joined'))).toBe(true);

      // Server-side push to the user lands on the private channel.
      const push = client.waitFor(
        (m) => m.type === 'event' && (m.event as DomainEvent).type === 'user.user-42.notified',
      );
      await gateway.toUser('user-42', 'notified', { text: 'hi' }, context);
      expect(await push).toBeDefined();

      // Forbidden channels are refused.
      client.send({ action: 'subscribe', channel: 'secrets' });
      await client.waitFor((m) => m.type === 'error' && m.code === 'FORBIDDEN_CHANNEL');

      // Unsubscribing leaves the channel and clears presence.
      client.send({ action: 'unsubscribe', channel: 'reports' });
      await client.waitFor((m) => m.type === 'unsubscribed' && m.channel === 'reports');
      expect(gateway.online('reports')).toEqual([]);
    } finally {
      client.close();
      await Bun.sleep(30);
      await gateway.close();
      await stop();
    }
  });

  test('refuses sockets when authentication returns null', async () => {
    const gateway = new WebsocketGateway({ bus: new MemoryEventBus(), authenticate: () => null });
    const { port, stop } = serve(gateway);
    try {
      const client = await connect(port);
      const close = new Promise<{ code: number }>((resolve) =>
        client.socket.addEventListener('close', (e) => resolve({ code: e.code }), { once: true }),
      );
      expect((await close).code).toBe(4001);
    } finally {
      await gateway.close();
      await stop();
    }
  });
});

import type { DomainEvent } from '../src/event';

void EXECUTION_CONTEXT;
void createDomainEvent;
