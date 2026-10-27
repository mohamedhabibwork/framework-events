import { RedisClient } from 'bun';
import { dispatch, type EventBus, type EventHandler, type HandlerErrorReporter, type Unsubscribe } from './bus';
import { type DomainEvent, domainEventSchema } from './event';

export interface RedisEventBusOptions {
  readonly url: string;
  /** Namespace so several apps can share one Redis without crosstalk. */
  readonly prefix: string;
  readonly report?: HandlerErrorReporter;
  readonly onInvalidMessage?: (channel: string, reason: string) => void;
}

/**
 * Cross-instance pub/sub over Redis. Uses a dedicated subscriber connection
 * (a subscribed Redis connection cannot run other commands) and one Redis
 * subscription per channel, fanned out to local handlers.
 */
export class RedisEventBus implements EventBus {
  readonly #publisher: RedisClient;
  readonly #subscriber: RedisClient;
  readonly #handlers = new Map<string, Set<EventHandler>>();
  #closed = false;

  constructor(private readonly options: RedisEventBusOptions) {
    this.#publisher = new RedisClient(options.url);
    this.#subscriber = new RedisClient(options.url);
  }

  #key(channel: string): string {
    return `${this.options.prefix}:events:${channel}`;
  }

  async publish(event: DomainEvent): Promise<void> {
    await this.#publisher.publish(this.#key(event.channel), JSON.stringify(event));
  }

  async subscribe(channel: string, handler: EventHandler): Promise<Unsubscribe> {
    let set = this.#handlers.get(channel);
    if (!set) {
      set = new Set<EventHandler>();
      this.#handlers.set(channel, set);
      await this.#subscriber.subscribe(this.#key(channel), (message) => this.#receive(channel, message));
    }
    set.add(handler);
    const handlers = set;
    return async () => {
      handlers.delete(handler);
      if (handlers.size === 0 && this.#handlers.get(channel) === handlers) {
        this.#handlers.delete(channel);
        if (!this.#closed) await this.#subscriber.unsubscribe(this.#key(channel));
      }
    };
  }

  #receive(channel: string, message: string): void {
    let parsed: ReturnType<typeof domainEventSchema.safeParse>;
    try {
      parsed = domainEventSchema.safeParse(JSON.parse(message));
    } catch {
      this.options.onInvalidMessage?.(channel, 'not JSON');
      return;
    }
    if (!parsed.success) {
      this.options.onInvalidMessage?.(channel, parsed.error.message);
      return;
    }
    void dispatch(this.#handlers.get(channel) ?? [], parsed.data, this.options.report);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#handlers.clear();
    this.#subscriber.close();
    this.#publisher.close();
  }
}
