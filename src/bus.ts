import { token } from '@framework/core';
import type { DomainEvent } from './event';

export type EventHandler = (event: DomainEvent) => void | Promise<void>;
export type Unsubscribe = () => Promise<void>;

/** Publish/subscribe contract shared by the in-memory and Redis adapters. */
export interface EventBus {
  publish(event: DomainEvent): Promise<void>;
  subscribe(channel: string, handler: EventHandler): Promise<Unsubscribe>;
  close(): Promise<void>;
}

export const EVENT_BUS = token<EventBus>('events.bus');

export type HandlerErrorReporter = (error: Error, event: DomainEvent) => void;

const defaultReporter: HandlerErrorReporter = (error, event) =>
  console.error(`[events] handler failed for ${event.type} (${event.id})`, error);

/** Runs handlers independently; one failing listener never blocks the others or the publisher. */
export async function dispatch(
  handlers: Iterable<EventHandler>,
  event: DomainEvent,
  report: HandlerErrorReporter = defaultReporter,
): Promise<void> {
  await Promise.all(
    [...handlers].map(async (handler) => {
      try {
        await handler(event);
      } catch (error) {
        report(error instanceof Error ? error : new Error(String(error)), event);
      }
    }),
  );
}

/** Single-process bus for development and tests (no cross-instance fan-out). */
export class MemoryEventBus implements EventBus {
  readonly #handlers = new Map<string, Set<EventHandler>>();

  constructor(private readonly report?: HandlerErrorReporter) {}

  async publish(event: DomainEvent): Promise<void> {
    await dispatch(this.#handlers.get(event.channel) ?? [], event, this.report);
  }

  async subscribe(channel: string, handler: EventHandler): Promise<Unsubscribe> {
    const set = this.#handlers.get(channel) ?? new Set<EventHandler>();
    set.add(handler);
    this.#handlers.set(channel, set);
    return async () => {
      set.delete(handler);
      if (set.size === 0) this.#handlers.delete(channel);
    };
  }

  async close(): Promise<void> {
    this.#handlers.clear();
  }
}
