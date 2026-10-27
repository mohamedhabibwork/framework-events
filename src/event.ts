import { randomUUID } from 'node:crypto';
import type { Actor, ExecutionContext, JsonValue } from '@framework/core';
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

export function createDomainEvent<TPayload extends JsonValue>(
  channel: string,
  verb: string,
  payload: TPayload,
  context: ExecutionContext,
): DomainEvent<TPayload> {
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
