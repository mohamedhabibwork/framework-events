export { websocket } from 'hono/bun';
export { dispatch, MemoryEventBus } from './bus';
export { EVENT_BUS } from './constants';
export { channelSchema, createDomainEvent, domainEventSchema } from './event';
export { type ChannelAuthorizer, type GatewayOptions, type SocketAuthenticator, WebsocketGateway } from './gateway';
export { RealtimeHub, realtimeRoutes } from './realtime';
export { RedisEventBus } from './redis-bus';
// Grouped conventions: every type in ./types, every constant in ./constants.
export type {
  DomainEvent,
  EventBus,
  EventHandler,
  HandlerErrorReporter,
  RealtimeOptions,
  RedisEventBusOptions,
  ServerMessage,
  Unsubscribe,
} from './types';
