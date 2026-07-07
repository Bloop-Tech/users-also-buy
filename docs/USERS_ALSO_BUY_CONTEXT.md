# bloop Recommendation Pipelines — Context

## What this repo does

This repo hosts **offline batch pipelines** that generate recommendation data for bloop. It contains two independent pipelines:

| Pipeline | Unit | Output |
|----------|------|--------|
| **Also Buy** (`pipelines/also_buy/`) | Per product | Complementary search queries stored on Marketplacer |
| **Exploration Themes** (`pipelines/exploration_themes/`) | Per theme | Curated product collections stored in Typesense |

See [EXPLORATION_THEMES.md](EXPLORATION_THEMES.md) for the themes pipeline.

---

## Also Buy pipeline

Powers bloop's **"users also buy"** recommendations. For each marketplace product, it generates **complementary search queries** (e.g. for a pair of sneakers → "socks", "shoe cleaner") and stores them on the product in **Marketplacer**.

Those queries are consumed downstream (via Typesense search) to surface related products on product pages. This pipeline is responsible for **generating and persisting** those queries — not for serving them at request time.

### Execution model

- Runs as an **Azure Function** (`users-also-buy-production-func`) on a **daily timer trigger** at **12:00 UTC** (`0 0 12 * * *`).
- Entry point: `TimerFunction/__init__.py` → `pipelines.also_buy.main.main()`.
- Deployed automatically on push to `main` / `deploy-azure` via GitHub Actions.

### Main flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Azure Blob     │────▶│  Marketplacer    │────▶│  Azure OpenAI   │
│  (checkpoint)   │     │  (fetch products)│     │  (LLM agent)    │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
        ▲                                                  │
        │                                                  ▼
        │               ┌──────────────────┐     ┌─────────────────┐
        └───────────────│  Azure Blob      │◀────│  Marketplacer   │
                        │  (update status) │     │  (save queries) │
                        └──────────────────┘     └─────────────────┘
```

### Key modules

| Module | Role |
|---|---|
| `pipelines/also_buy/main.py` | Pipeline orchestration |
| `src/also_buy/agent.py` | Pydantic AI agent (`gpt-5-mini`) |
| `src/also_buy/marketplacer_gateway.py` | GraphQL client |
| `src/also_buy/pipeline.py` | Shared generate + write helpers |
| `src/common/azure_blob_client.py` | Checkpoint blob storage |

### Environment variables

| Variable | Used by |
|---|---|
| `MARKETPLACER_URL`, `MARKETPLACER_TOKEN` | Marketplacer GraphQL |
| `AZURE_OPENAI_*` | LLM agent |
| `AZURE_STORAGE_CONNECTION_STRING` | Pipeline checkpoint blob |
| `EMBEDDINGS_SERVICE_URL`, `TYPESENSE_*` | Search tools |

### Supporting tools

- `pipelines/also_buy/main_adhoc_products.py` — Reprocess hardcoded product IDs
- `tools/streamlit_also_buy/app.py` — Compare agent variants + preview search hits
