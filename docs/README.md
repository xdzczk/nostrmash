# Documentation

This directory is the working map for NostrMash. Use it when the top-level `README.md` is no longer enough and you need the repo's actual architecture, development workflow, operating model, or API semantics.

## Choose Your Starting Point

| If you are... | Start here | Why |
| --- | --- | --- |
| A new contributor | [development.md](development.md) | Local setup, commands, migrations, tests, and projection safety rails |
| An operator | [operations.md](operations.md) | Health, checkpoints, jobs, rebuilds, and troubleshooting |
| An API integrator | [api.md](api.md) | API surfaces, consistency model, pagination, and links to the contract |
| Reviewing the system design | [architecture.md](architecture.md) | Service boundaries, data flow, layering, and rebuild philosophy |

## Recommended Reading Order

1. [architecture.md](architecture.md)
2. [development.md](development.md) or [operations.md](operations.md), depending on your role
3. [api.md](api.md)
4. [openapi.yaml](openapi.yaml) for request and response details

## Source Of Truth Map

Use one canonical home for each topic:

- Landing page and first boot: [`../README.md`](../README.md)
- Contributor workflows: [development.md](development.md)
- Runtime health and incident response: [operations.md](operations.md)
- API semantics and consistency: [api.md](api.md)
- Primal compatibility target and gaps: [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- Compatibility migration staging: [compatibility_rollout.md](compatibility_rollout.md)
- Request and response schema details: [openapi.yaml](openapi.yaml)

## Related

- [`../README.md`](../README.md)
- [architecture.md](architecture.md)
- [development.md](development.md)
- [operations.md](operations.md)
- [api.md](api.md)
- [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- [compatibility_rollout.md](compatibility_rollout.md)
