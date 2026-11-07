/**
 * Compiled by its own `tsc` program (see `register.test.ts`): augmenting
 * `Register` is global to a program, so it cannot live in the monorepo's.
 */
import type { ExecutionContext } from '@framework/core';
import { createDomainEvent, type EventBus } from '@framework/events';

type AppEvents = {
  'orders.created': { readonly orderId: string };
  'orders.cancelled': { readonly reason: string };
};

declare module '@framework/core' {
  interface Register {
    events: AppEvents;
  }
}

export async function useEvents(bus: EventBus, context: ExecutionContext): Promise<void> {
  await bus.publish(createDomainEvent('orders', 'created', { orderId: 'o-1' }, context));
  // @ts-expect-error orders.created carries an orderId
  createDomainEvent('orders', 'created', { nope: true }, context);
  // Unregistered events stay open: CRUD's entity events are not declared by every app.
  createDomainEvent('products', 'created', { anything: 1 }, context);
  const channel: string = 'dynamic';
  createDomainEvent(channel, 'created', { anything: 1 }, context);

  await bus.subscribe('orders', async (event) => {
    if (event.type === 'orders.created') {
      const id: string = event.payload.orderId;
      expectString(id);
    }
    // @ts-expect-error orders.cancelled has no orderId
    if (event.type === 'orders.cancelled') expectString(event.payload.orderId);
  });
  await bus.subscribe('products', async (event) => {
    expectString(event.type);
  });
}

function expectString(_value: string): void {}
