# Exploration Themes Pipeline

## Product context

bloop is extending its social feed so users can create posts recommending products they **did not buy**. In the exploration flow, users upload a video and attach products from themed collections.

Each collection is a curated set of marketplace products around a social-post-friendly theme (e.g. "Beach day with your girls", "Airfryer of your dreams"). Users browse collections, select one or more products from each, and publish.

## What this repo owns

This pipeline **generates and persists** exploration theme collections. It does **not** serve them at request time — the frontend reads from Typesense.

| Responsibility | Owner |
|----------------|-------|
| Theme definitions + curation | This pipeline |
| Theme + product serving | Frontend / API |
| Product details at serve time | `products_v2` Typesense collection |

## Per-theme pipeline

```
```
THEMES_FILE (.env)  →  LLM queries  →  Typesense search (PRODUCTS_COLLECTION)
                                                      ↓
                                     LLM selection  →  quality gates  →  embed
                                                      ↓
                    EXPLORATION_THEMES_COLLECTION upsert + update THEMES_FILE
```

### Steps (per theme)

1. Load theme from `THEMES_FILE` (or generate new themes in `expand` mode)
2. LLM generates 2–4 keyword search queries
3. Hybrid Typesense search per query; merge and dedupe candidates (~50 max)
4. LLM selects 8–15 matching products from candidates
5. Quality gates (min/max products, valid IDs, brand diversity)
6. Embed `title_en + description + tags` for personalization
7. Upsert to Typesense `exploration_themes`; write back yaml

## Iteration modes

### `expand` — add new themes

1. LLM generates N new themes with `title_en`, `title_pt`, `title_es`
2. Dedup against existing `title_en` values (embedding similarity)
3. Append to `THEMES_FILE` with `status: pending`
4. Run full pipeline for pending themes

### `refresh` — re-curate existing themes

1. Load themes from `THEMES_FILE`
2. Re-run queries → search → selection
3. Bump version, update Typesense + yaml

## Theme data model

`title_en` is the **canonical theme name** — used for slug generation and embeddings.

| Field | Stored in yaml | Stored in Typesense |
|-------|----------------|---------------------|
| `id` | yes | yes |
| `title_en`, `title_pt`, `title_es` | yes | yes |
| `description`, `tags`, `category`, `audience`, `season` | yes | yes |
| `search_queries`, `product_ids` | yes | yes |
| `status`, `version`, `updated_at` | yes | yes |
| `embedding` | no | yes |

### Local theme file

`THEMES_FILE` selects the active local theme YAML file. For example, you can point dev and prod to different files because curated `product_ids` differ by environment. Review diffs after expand runs.

## Typesense collection: `exploration_themes`

Create manually or via `scripts/create_exploration_themes_collection.py`:

| Field | Type | Notes |
|-------|------|-------|
| `id` | string | slug, facet |
| `title_en`, `title_pt`, `title_es` | string | localized titles |
| `description` | string | |
| `tags` | string[] | facet |
| `category`, `audience`, `season` | string | facet |
| `search_queries` | string[] | |
| `product_ids` | string[] | |
| `product_count` | int32 | |
| `status` | string | facet: active, insufficient_products, deprecated |
| `version` | int32 | |
| `updated_at` | int64 | unix timestamp |
| `embedding` | float[] | HNSW, same dims as product embeddings |

## Quality gates

| Gate | Rule |
|------|------|
| Min products | ≥ 8 or `status=insufficient_products` |
| Max products | cap at 15 |
| Valid IDs | must be in search candidates |
| Diversity | max 3 per brand |
| Publish | only `active` themes upserted to Typesense |

## Serve-time contract (frontend)

1. Vector-search `exploration_themes` by user interest embedding
2. Display localized title (`title_pt` / `title_es` based on locale)
3. Hydrate `product_ids` from `products_v2`
4. Filter products by user locale at serve time

## Running locally

Swap the active `.env` file depending on the target environment. The active `.env` controls `APP_ENV`, `PRODUCTS_COLLECTION`, `EXPLORATION_THEMES_COLLECTION`, and `THEMES_FILE`.

```bash
# Validate local setup (no API calls)
uv run python scripts/validate_exploration_themes_setup.py

# Create the configured Typesense collection (requires TYPESENSE_* env vars)
uv run python scripts/create_exploration_themes_collection.py

# Expand with new themes using the active .env
uv run python -m pipelines.exploration_themes.main_adhoc --mode expand --expand-count 30

# Refresh all themes using the active .env
uv run python -m pipelines.exploration_themes.main_adhoc --mode refresh

# Refresh one theme using the active .env
uv run python -m pipelines.exploration_themes.main_adhoc --mode refresh --theme-ids beach-day-with-friends

# QA UI
uv run streamlit run tools/streamlit_themes/app.py
```

Example environment-specific values:

```env
# .env.dev
APP_ENV=development
PRODUCTS_COLLECTION=products_v2_dev
EXPLORATION_THEMES_COLLECTION=exploration_themes_dev
THEMES_FILE=data/themes.dev.yaml
```

```env
# .env.prod
APP_ENV=production
PRODUCTS_COLLECTION=products_v2
EXPLORATION_THEMES_COLLECTION=exploration_themes
THEMES_FILE=data/themes.prod.yaml
```

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `APP_ENV` | `development` | Logging and production warning |
| `AZURE_OPENAI_*` | — | LLM agents |
| `TYPESENSE_*` | — | Search + write |
| `EMBEDDINGS_SERVICE_URL` | — | Theme embeddings |
| `PRODUCTS_COLLECTION` | `products_v2` | Source Typesense product collection |
| `EXPLORATION_THEMES_COLLECTION` | `exploration_themes` | Theme Typesense collection |
| `THEMES_FILE` | `data/themes.yaml` | Local theme store |

## Operational notes

- v1 runs **manually** via adhoc CLI; Azure timer planned post-QA
- The active `THEMES_FILE` is version-controlled; no blob storage for themes
- When `APP_ENV=production`, the adhoc CLI prints a large warning banner and logs the active config values before running
- Also-buy daily pipeline is unaffected (separate entry point)
