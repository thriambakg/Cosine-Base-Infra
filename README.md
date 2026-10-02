# Cosine Base Infrastructure

Shared AWS infrastructure for FinGov / Cosine: authentication, storage, encryption, monitoring, and data-pipeline foundations used across environments.

**Product:** [fingov.ai](https://fingov.ai)

## Product design case study

How the product was designed end-to-end (problem, tradeoffs, IA, UI, outcomes) — including how infra constraints (batch vs on-demand APIs, indexing, solo ops) shaped the UX:

**→ [FinGov Product Design Case Study](docs/FinGov-Product-Design-Case-Study.md)**

Screenshots for the write-up live under [`docs/case-study/screenshots/`](docs/case-study/screenshots/).

## Infrastructure docs

Terraform modules, environments, and deployment steps:

**→ [Infrastructure README](docs/README.md)**

## Repository

| Path | Contents |
|------|----------|
| `terraform/` | Shared modules and environment configs |
| `backend_app/` | Glue jobs and shared backend workers |
| `docs/` | Infra guides + product design case study |
| `static-files/` | Static assets for shared use |
| `scripts/` | Ops helpers |

Application UI and app-tier services live in the companion **Cosine2.0** repository.
