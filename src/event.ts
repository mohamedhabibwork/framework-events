import { randomUUID } from 'node:crypto';
import type { Actor, ExecutionContext, JsonValue, Register } from '@framework/core';
import { z } from 'zod/v4';

/** A fact that happened, published after the change is committed. */
export interface DomainEvent<TPayload extends JsonValue = JsonValue> {
  readonly id: string;
  /** `<channel>.<verb>`, e.g. `products.created`. */
  readonly type: string;
  /** Fan-out channel, usually the resource name. */
  readonly channel: string;
  readonly payload: TPayload;
  readonly correlationId: string;
  readonly actor: Actor;
  readonly occurredAt: string;
}

type RegisteredEvents = Register extends { readonly events: infer TEvents extends object } ? TEvents : object;
type RegisteredEventType = keyof RegisteredEvents & string;

/** The payload `Register.events` declares for `TType`; any JSON value for an event it doesn't list. */
export type EventPayload<TType extends string> = TType extends RegisteredEventType
  ? Extract<RegisteredEvents[TType], JsonValue>
  : JsonValue;

type EventTypeOn<TChannel extends string> = Extract<RegisteredEventType, `${TChannel}.${string}`>;

/** What a handler on `TChannel` receives: the declared events, or any event when none are declared. */
export type EventOn<TChannel extends string> = [EventTypeOn<TChannel>] extends [never]
  ? DomainEvent
  : {
      [TType in EventTypeOn<TChannel>]: Omit<DomainEvent<EventPayload<TType>>, 'type'> & { readonly type: TType };
    }[EventTypeOn<TChannel>];

const CHANNEL = /^[a-z][a-z0-9_.:-]{0,127}$/;

export const channelSchema = z.string().regex(CHANNEL, 'Invalid channel name');

const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValueSchema),
    z.record(z.string(), jsonValueSchema),
  ]),
);

/** Validates events crossing process boundaries (Redis messages). */
export const domainEventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  channel: channelSchema,
  payload: jsonValueSchema,
  correlationId: z.string(),
  actor: z.object({ id: z.string().nullable(), type: z.enum(['user', 'service', 'system', 'anonymous']) }),
  occurredAt: z.iso.datetime(),
});

export function createDomainEvent<
  const TChannel extends string,
  const TVerb extends string,
  TPayload extends EventPayload<`${TChannel}.${TVerb}`>,
>(channel: TChannel, verb: TVerb, payload: TPayload, context: ExecutionContext): DomainEvent<TPayload> {
  channelSchema.parse(channel);
  return {
    id: randomUUID(),
    type: `${channel}.${verb}`,
    channel,
    payload,
    correlationId: context.correlationId,
    actor: context.actor,
    occurredAt: new Date().toISOString(),
  };
}
