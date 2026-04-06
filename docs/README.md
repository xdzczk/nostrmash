# Documentation

This directory is the working map for NostrMash. Use it when the top-level `README.md` is no longer enough and you need the repo's actual architecture, development workflow, operating model, or API semantics.

## Choose Your Starting Point

| If you are... | Start here | Why |
| --- | --- | --- |
| A new contributor | [../CONTRIBUTING.md](../CONTRIBUTING.md) | Entrypoint workflow, change-type playbook, and links to deep docs |
| An operator | [operations.md](operations.md) | Health, checkpoints, jobs, rebuilds, and troubleshooting |
| An API integrator | [api.md](api.md) | API surfaces, consistency model, pagination, and links to the contract |
| Reviewing the system design | [architecture.md](architecture.md) | Service boundaries, data flow, layering, and rebuild philosophy |
| Planning trust/ranking work | [architecture/trust-subsystem.md](architecture/trust-subsystem.md) | Target WoT/ranking subsystem shape, Redis role, and publication model |

## Recommended Reading Order

1. [architecture.md](architecture.md)
2. [../CONTRIBUTING.md](../CONTRIBUTING.md) for contributor workflow, then [development.md](development.md) or [operations.md](operations.md)
3. [api.md](api.md)
4. [openapi.yaml](openapi.yaml) for request and response details

## Source Of Truth Map

Use one canonical home for each topic:

- Landing page and first boot: [`../README.md`](../README.md)
- Contributor entrypoint and change workflow: [`../CONTRIBUTING.md`](../CONTRIBUTING.md)
- Local dev environment and loops: [development.md](development.md)
- Coolify deployment with managed Postgres/Redis: [coolify.md](coolify.md)
- Environment-variable reference: [configuration.md](configuration.md)
- Testing gates and targeted validation: [testing.md](testing.md)
- Runtime health and incident response: [operations.md](operations.md)
- Observability signals and alert interpretation: [observability.md](observability.md)
- Performance hot paths and benchmark/load evidence: [performance.md](performance.md)
- API semantics and consistency: [api.md](api.md)
- Compatibility and deprecation policy: [compatibility.md](compatibility.md)
- Migration safety and rollback-aware schema guidance: [migrations.md](migrations.md)
- Primal compatibility target and gaps: [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- Compatibility migration staging: [compatibility_rollout.md](compatibility_rollout.md)
- Trust/ranking target design: [architecture/trust-subsystem.md](architecture/trust-subsystem.md)
- Request and response schema details: [openapi.yaml](openapi.yaml)
- Release execution: [`../RELEASE.md`](../RELEASE.md)
- Versioning and tag contract: [`../VERSIONING.md`](../VERSIONING.md)

## Related

- [`../README.md`](../README.md)
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md)
- [architecture.md](architecture.md)
- [architecture/trust-subsystem.md](architecture/trust-subsystem.md)
- [development.md](development.md)
- [coolify.md](coolify.md)
- [configuration.md](configuration.md)
- [testing.md](testing.md)
- [operations.md](operations.md)
- [observability.md](observability.md)
- [performance.md](performance.md)
- [migrations.md](migrations.md)
- [compatibility.md](compatibility.md)
- [api.md](api.md)
- [primal_compatibility_matrix.md](primal_compatibility_matrix.md)
- [compatibility_rollout.md](compatibility_rollout.md)
- [`../RELEASE.md`](../RELEASE.md)
- [`../VERSIONING.md`](../VERSIONING.md)
