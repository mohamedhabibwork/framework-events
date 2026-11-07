import type { Actor } from '@framework/core';
import { ANONYMOUS_ACTOR } from '@framework/core';
import type { Context } from 'hono';
import { Hono } from 'hono';
import { upgradeWebSocket } from 'hono/bun';
import type { WSContext } from 'hono/ws';
import { z } from 'zod/v4';
import type { EventBus } from './bus';
import type { DomainEvent } from './event';
import { createDomainEvent } from './event';
import { RealtimeHub } from './realtime';

/** Who is connecting; return null to refuse the socket entirely. */
export type SocketAuthenticator = (c: Context) => Actor | null | Promise<Actor | null>;

/** Per-connection channel authorization (called after authentication). */
export type ChannelAuthorizer = (c: Context, channel: string, actor: Actor) => boolean | Promise<boolean>;

export interface GatewayOptions {
  /** The bus the gateway multiplexes and publishes to. */
  readonly bus: EventBus;
  /** Who is connecting; null refuses the socket (default: anonymous). */
  readonly authenticate?: SocketAuthenticator;
  /** Per-connection channel authorization (default: allowlisted channels only). */
  readonly authorize?: ChannelAuthorizer;
  /** Channels any authenticated connection may join (default: ['events']). */
  readonly channels?: readonly string[];
  /** Track online actors per channel and broadcast presence events. */
  readonly presence?: boolean;
  readonly maxSubscriptions?: number;
}

const identity = (socket: WSContext): object => (socket.raw as object | undefined) ?? socket;

const clientMessage = z.discriminatedUnion('action', [
  z.object({ action: z.literal('subscribe'), channel: z.string().min(1).max(128) }),
  z.object({ action: z.literal('unsubscribe'), channel: z.string().min(1).max(128) }),
  z.object({ action: z.literal('ping') }),
]);

/**
 * WebSocket gateway over the event bus (Laravel Echo-style): connection-time
 * authentication, automatic private `user.<id>` channels for authenticated
 * actors, per-connection channel authorization, and per-channel presence with
 * `presence.joined` / `presence.left` broadcasts.
 *
 * Protocol (JSON text frames):
 * `{action:'subscribe',channel}` → `{type:'subscribed',channel}`
 * `{action:'unsubscribe',channel}` → `{type:'unsubscribed',channel}`
 * `{type:'event',event}` per matching bus event · `{action:'ping'}` → pong
 *
 * Server-side push: `gateway.toUser(id, …)` and `gateway.broadcast(ch, …)`.
 */
export class WebsocketGateway {
  readonly #hub: RealtimeHub;
  readonly #options: GatewayOptions & {
    readonly channels: readonly string[];
    readonly presence: boolean;
    readonly maxSubscriptions: number;
  };
  /** socket identity → the actor that authenticated it. */
  readonly #actors = new Map<object, Actor>();
  /** channel → actor ids currently subscribed (presence). */
  readonly #presence = new Map<string, Set<string>>();

  constructor(options: GatewayOptions) {
    this.#options = { channels: ['events'], presence: false, maxSubscriptions: 20, ...options };
    this.#hub = new RealtimeHub(options.bus);
  }

  /** The actor that authenticated a socket (anonymous when no auth configured). */
  actorOf(socket: WSContext): Actor {
    return this.#actors.get(identity(socket)) ?? ANONYMOUS_ACTOR;
  }

  /** Online actor ids for a channel (when presence tracking is on). */
  online(channel: string): readonly string[] {
    return [...(this.#presence.get(channel) ?? [])];
  }

  /** Server-side push to one user's private `user.<id>` channel. */
  async toUser(
    userId: string,
    verb: string,
    payload: DomainEvent['payload'],
    execution: Parameters<typeof createDomainEvent>[3],
  ): Promise<void> {
    await this.#options.bus.publish(createDomainEvent(`user.${userId}`, verb, payload, execution));
  }

  /** Server-side broadcast to a channel. */
  async broadcast(
    channel: string,
    verb: string,
    payload: DomainEvent['payload'],
    execution: Parameters<typeof createDomainEvent>[3],
  ): Promise<void> {
    await this.#options.bus.publish(createDomainEvent(channel, verb, payload, execution));
  }

  /** Closes every channel subscription and clears presence (call on shutdown). */
  async close(): Promise<void> {
    await this.#hub.close();
    this.#actors.clear();
    this.#presence.clear();
  }

  /** The native Hono WebSocket route (Bun); mount it at e.g. `/ws`. */
  routes(): Hono {
    // Lexical alias: the upgrade hooks are plain objects, so `this` inside
    // them is not the gateway; `gateway` keeps private-field access.
    const gateway = this;
    const send = (socket: WSContext, message: object) => socket.send(JSON.stringify(message));

    return new Hono().get(
      '/',
      upgradeWebSocket((c) => {
        const joined = new Set<string>();
        let actor: Actor = ANONYMOUS_ACTOR;
        let admitted = true;

        const executionFor = (): Parameters<typeof createDomainEvent>[3] => ({
          kind: 'command',
          correlationId: `ws-${Date.now()}`,
          actor,
          startedAt: new Date(),
          withActor: (next: Actor) => {
            actor = next;
            return executionFor();
          },
        });

        const isAllowed = async (channel: string): Promise<boolean> => {
          const base =
            gateway.#options.channels.includes(channel) ||
            (actor.id !== null && (channel === `user.${actor.id}` || channel.startsWith(`user.${actor.id}.`)));
          if (!base) return false;
          return gateway.#options.authorize ? gateway.#options.authorize(c, channel, actor) : true;
        };

        const presenceChange = async (channel: string, joinedNow: boolean) => {
          if (!gateway.#options.presence || actor.id === null) return;
          const set = gateway.#presence.get(channel) ?? new Set<string>();
          if (joinedNow) {
            if (set.has(actor.id)) return;
            set.add(actor.id);
            gateway.#presence.set(channel, set);
            await gateway.broadcast(channel, 'presence.joined', { channel, actorId: actor.id }, executionFor());
          } else {
            set.delete(actor.id);
            if (set.size === 0) gateway.#presence.delete(channel);
            await gateway.broadcast(channel, 'presence.left', { channel, actorId: actor.id }, executionFor());
          }
        };

        const join = async (socket: WSContext, channel: string) => {
          if (joined.has(channel)) return;
          if (joined.size >= gateway.#options.maxSubscriptions) {
            send(socket, {
              type: 'error',
              code: 'TOO_MANY_SUBSCRIPTIONS',
              message: `At most ${gateway.#options.maxSubscriptions} channels`,
            });
            return;
          }
          joined.add(channel);
          await gateway.#hub.join(channel, socket);
          send(socket, { type: 'subscribed', channel });
          await presenceChange(channel, true);
        };

        const leave = async (socket: WSContext, channel: string) => {
          if (!joined.delete(channel)) return;
          await gateway.#hub.leave(channel, socket);
          send(socket, { type: 'unsubscribed', channel });
          await presenceChange(channel, false);
        };

        return {
          async onOpen(_event, socket) {
            actor = (await gateway.#options.authenticate?.(c)) ?? ANONYMOUS_ACTOR;
            // An explicit null from `authenticate` refuses the socket.
            if (gateway.#options.authenticate && actor.id === null && actor.type === 'anonymous') {
              admitted = false;
            }
            if (!admitted) {
              socket.close(4001, 'Unauthenticated');
              return;
            }
            gateway.#actors.set(identity(socket), actor);
            if (actor.id !== null) await join(socket, `user.${actor.id}`);
          },
          async onMessage(message, socket) {
            let parsed: ReturnType<typeof clientMessage.safeParse>;
            try {
              parsed = clientMessage.safeParse(JSON.parse(String(message.data)));
            } catch {
              return send(socket, { type: 'error', code: 'INVALID_MESSAGE', message: 'Messages must be JSON' });
            }
            if (!parsed.success) {
              return send(socket, { type: 'error', code: 'INVALID_MESSAGE', message: 'Unknown message' });
            }
            const data = parsed.data;
            if (data.action === 'ping') return send(socket, { type: 'pong' });
            if (data.action === 'unsubscribe') return leave(socket, data.channel);
            if (!(await isAllowed(data.channel))) {
              return send(socket, {
                type: 'error',
                code: 'FORBIDDEN_CHANNEL',
                message: `Cannot subscribe to ${data.channel}`,
              });
            }
            await join(socket, data.channel);
          },
          async onClose(_event, socket) {
            gateway.#actors.delete(identity(socket));
            for (const channel of [...joined]) await leave(socket, channel);
          },
        };
      }),
    );
  }
}
