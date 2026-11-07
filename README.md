# @framework/events

Typed event bus (in-memory and Redis pub/sub) and WebSocket realtime hub.

> **Read-only subtree split** — This package is developed inside the main [**mohamedhabibwork/framework**](https://github.com/mohamedhabibwork/framework/tree/main/packages/events) monorepo and automatically synced to [`mohamedhabibwork/framework-events`](https://github.com/mohamedhabibwork/framework-events). Please open issues and pull requests in the main framework repository.

## Ecosystem Links

- **Main Framework Repository**: [mohamedhabibwork/framework](https://github.com/mohamedhabibwork/framework)
- **Package Repository**: [mohamedhabibwork/framework-events](https://github.com/mohamedhabibwork/framework-events)
- **Starter Application Template**: [mohamedhabibwork/framework-app](https://github.com/mohamedhabibwork/framework-app)
- **Documentation Repository**: [mohamedhabibwork/framework-docs](https://github.com/mohamedhabibwork/framework-docs)
- **Documentation Site**: [https://framework.habib.cloud](https://framework.habib.cloud)

## Installation

### Inside a Standalone App (via GitHub)

```json
{
  "dependencies": {
    "@framework/events": "github:mohamedhabibwork/framework-events"
  }
}
```

```sh
bun install
```

### Creating a New Application

Use the starter application template repository to bootstrap a full application with all framework packages pre-wired:

```sh
gh repo create my-api --template mohamedhabibwork/framework-app --clone
cd my-api
bun install
```

## License

MIT
