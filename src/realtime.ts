import { type Context, type Env, Hono } from 'hono';
import { upgradeWebSocket } from 'hono/bun';
import type { WSContext } from 'hono/ws';
import { z } from 'zod/v4';
import type { EventBus, Unsubscribe } from './bus';
import { channelSchema, type DomainEvent } from './event';

type Socket = WSContext;

/** Hono wraps each callback's socket in a new WSContext; the raw Bun socket is the stable identity. */
const identity = (socket: Socket): object => (socket.raw as object | undefined) ?? socket;

const clientMessageSchema = z.discriminatedUnion('action', [
  z.object({ action: z.literal('subscribe'), channel: channelSchema }),
  z.object({ action: z.literal('unsubscribe'), channel: channelSchema }),
  z.object({ action: z.literal('ping') }),
]);

export type ServerMessage =
  | { readonly type: 'subscribed' | 'unsubscribed'; readonly channel: string }
  | { readonly type: 'event'; readonly event: DomainEvent }
  | { readonly type: 'pong' }
  | {
      readonly type: 'error';
      readonly code: 'INVALID_MESSAGE' | 'FORBIDDEN_CHANNEL' | 'TOO_MANY_SUBSCRIPTIONS';
      readonly message: string;
    };

/**
 * Multiplexes WebSocket subscribers onto the event bus: one bus subscription
 * per channel no matter how many sockets listen, released when the last
 * socket leaves.
 */
export class RealtimeHub {
  readonly #sockets = new Map<string, Map<object, Socket>>();
  readonly #unsubscribe = new Map<string, Promise<Unsubscribe>>();

  constructor(private readonly bus: EventBus) {}

  async join(channel: string, socket: Socket): Promise<void> {
    let sockets = this.#sockets.get(channel);
    if (!sockets) {
      sockets = new Map();
      this.#sockets.set(channel, sockets);
      this.#unsubscribe.set(
        channel,
        this.bus.subscribe(channel, (event) => this.#broadcast(channel, { type: 'event', event })),
      );
    }
    sockets.set(identity(socket), socket);
    await this.#unsubscribe.get(channel);
  }

  async leave(channel: string, socket: Socket): Promise<void> {
    const sockets = this.#sockets.get(channel);
    if (!sockets?.delete(identity(socket)) || sockets.size > 0) return;
    this.#sockets.delete(channel);
    const unsubscribe = this.#unsubscribe.get(channel);
    this.#unsubscribe.delete(channel);
    await (await unsubscribe)?.();
  }

  listeners(channel: string): number {
    return this.#sockets.get(channel)?.size ?? 0;
  }

  #broadcast(channel: string, message: ServerMessage): void {
    const text = JSON.stringify(message);
    for (const socket of this.#sockets.get(channel)?.values() ?? []) socket.send(text);
  }

  async close(): Promise<void> {
    const pending = [...this.#unsubscribe.values()];
    this.#sockets.clear();
    this.#unsubscribe.clear();
    await Promise.all(pending.map(async (p) => (await p)()));
  }
}

export interface RealtimeOptions<E extends Env> {
  readonly hub: RealtimeHub;
  /** Channels a client may subscribe to (allowlist). */
  readonly channels: readonly string[];
  /** Per-connection authorization of a channel (default: allowlisted channels are public). */
  readonly authorize?: (c: Context<E>, channel: string) => boolean | Promise<boolean>;
  readonly maxSubscriptions?: number;
}

const send = (socket: Socket, message: ServerMessage) => socket.send(JSON.stringify(message));

/**
 * Native Hono WebSocket route (Bun). The server must pass `websocket` from
 * `hono/bun` to `Bun.serve`. Protocol: `{action:"subscribe",channel}` ->
 * `{type:"subscribed"}`, then `{type:"event",event}` per published event.
 */
export function realtimeRoutes<E extends Env>(options: RealtimeOptions<E>) {
  const max = options.maxSubscriptions ?? 20;
  return new Hono<E>().get(
    '/',
    upgradeWebSocket((c) => {
      const joined = new Set<string>();
      return {
        async onMessage(message, socket) {
          let parsed: z.ZodSafeParseResult<z.output<typeof clientMessageSchema>>;
          try {
            parsed = clientMessageSchema.safeParse(JSON.parse(String(message.data)));
          } catch {
            return send(socket, { type: 'error', code: 'INVALID_MESSAGE', message: 'Messages must be JSON' });
          }
          if (!parsed.success)
            return send(socket, { type: 'error', code: 'INVALID_MESSAGE', message: 'Unknown message' });
          const data = parsed.data;
          if (data.action === 'ping') return send(socket, { type: 'pong' });
          if (data.action === 'unsubscribe') {
            joined.delete(data.channel);
            await options.hub.leave(data.channel, socket);
            return send(socket, { type: 'unsubscribed', channel: data.channel });
          }
          const allowed =
            options.channels.includes(data.channel) &&
            ((await options.authorize?.(c as Context<E>, data.channel)) ?? true);
          if (!allowed) {
            return send(socket, {
              type: 'error',
              code: 'FORBIDDEN_CHANNEL',
              message: `Cannot subscribe to ${data.channel}`,
            });
          }
          if (!joined.has(data.channel) && joined.size >= max) {
            return send(socket, { type: 'error', code: 'TOO_MANY_SUBSCRIPTIONS', message: `At most ${max} channels` });
          }
          joined.add(data.channel);
          await options.hub.join(data.channel, socket);
          send(socket, { type: 'subscribed', channel: data.channel });
        },
        async onClose(_event, socket) {
          await Promise.all([...joined].map((channel) => options.hub.leave(channel, socket)));
          joined.clear();
        },
      };
    }),
  );
}
