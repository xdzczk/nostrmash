# Documentation

This is the docs hub for NostrMash. Use the top-level [`README.md`](../README.md) for the project overview and first boot. Use this page when you want the shortest path to the right document without guessing.

## Reader journeys

| If you are... | Start here | Then read |
| --- | --- | --- |
| New to the system | [architecture.md](architecture.md) | [api.md](api.md) |
| Building locally | [development.md](development.md) | [testing.md](testing.md) |
| Deploying to production | [coolify.md](coolify.md) | [operations.md](operations.md) |
| Operating the stack | [operations.md](operations.md) | [observability.md](observability.md) |
| Integrating with the API | [api.md](api.md) | [openapi.yaml](openapi.yaml) |
| Changing compatibility behavior | [primal_compatibility_matrix.md](primal_compatibility_matrix.md) | [compatibility_rollout.md](compatibility_rollout.md) |
| Planning trust and ranking work | [architecture/trust-subsystem.md](architecture/trust-subsystem.md) | [architecture.md](architecture.md) |
| Contributing safely | [../CONTRIBUTING.md](../CONTRIBUTING.md) | [testing.md](testing.md) |
| Releasing or managing policy | [../RELEASE.md](../RELEASE.md) | [compatibility.md](compatibility.md) |

## Documentation tiers

### Tier 1: start here first

These pages are meant to orient you, not just answer a narrow lookup:

- [`../README.md`](../README.md): product overview, quick start, and primary paths
- [architecture.md](architecture.md): system model, layers, and service boundaries
- [api.md](api.md): API surface selection and consistency model
- [operations.md](operations.md): runtime and incident-response guide
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md): contributor workflow and safety expectations

### Tier 2: lookup and deep reference

These pages are authoritative, but they are best used as targeted references:

- [configuration.md](configuration.md): generated environment-variable reference
- [observability.md](observability.md): metrics, tracing, and signal interpretation
- [development.md](development.md): local workflow details and replay-safe development loops
- [testing.md](testing.md): CI gates, race policy, coverage, fuzzing, benchmarks, and contract drift
- [migrations.md](migrations.md): schema safety and rollout posture
- [performance.md](performance.md): hot-path ownership and evidence collection
- [hotspot-watchlist.md](hotspot-watchlist.md): maintainability risk watchlist for structurally fragile code surfaces
- [compatibility.md](compatibility.md): compatibility and deprecation policy
- [primal_compatibility_matrix.md](primal_compatibility_matrix.md): exact compatibility inventory
- [compatibility_rollout.md](compatibility_rollout.md): staged adoption plan
- [coolify.md](coolify.md): production-oriented Coolify deployment path
- [architecture/trust-subsystem.md](architecture/trust-subsystem.md): trust and ranking design
- [architecture/orchestration-surfaces.md](architecture/orchestration-surfaces.md): transport/query ownership map
- [`../RELEASE.md`](../RELEASE.md), [`../VERSIONING.md`](../VERSIONING.md), [`../SECURITY.md`](../SECURITY.md): maintainer policy and release guidance

## Source-of-truth map

Each topic has one canonical home:

| Topic | Canonical doc |
| --- | --- |
| Project overview and first boot | [`../README.md`](../README.md) |
| Contributor entrypoint | [`../CONTRIBUTING.md`](../CONTRIBUTING.md) |
| Local development loops | [development.md](development.md) |
| Production deployment on Coolify | [coolify.md](coolify.md) |
| Environment variables | [configuration.md](configuration.md) |
| Runtime health and response playbooks | [operations.md](operations.md) |
| Metrics, traces, and alerts | [observability.md](observability.md) |
| API mental model | [api.md](api.md) |
| Exact HTTP and response contract | [openapi.yaml](openapi.yaml) |
| Compatibility inventory | [primal_compatibility_matrix.md](primal_compatibility_matrix.md) |
| Compatibility operations | [compatibility_rollout.md](compatibility_rollout.md) |
| Compatibility and deprecation policy | [compatibility.md](compatibility.md) |
| Migration safety | [migrations.md](migrations.md) |
| Performance ownership and evidence | [performance.md](performance.md) |
| Maintainability hotspot watchlist | [hotspot-watchlist.md](hotspot-watchlist.md) |
| Trust subsystem design | [architecture/trust-subsystem.md](architecture/trust-subsystem.md) |

## Notes

- [configuration.md](configuration.md) is generated and works best as a lookup reference, not as a page to read front to back.
- Design notes that are useful but not first-class docs live under `docs/design/`; for example, see [design/storage-optimization.md](design/storage-optimization.md) for the scoped storage and relay-fallback note.
