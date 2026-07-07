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
data/themes.yaml  →  LLM queries  →  Typesense search  →  LLM selection
                                                              ↓
                                         quality gates  →  embed  →  Typesense upsert
                                                              ↓
                                                    update data/themes.yaml
```

### Steps (per theme)

1. Load theme from `data/themes.yaml` (or generate new themes in `expand` mode)
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
3. Append to `data/themes.yaml` with `status: pending`
4. Run full pipeline for pending themes

### `refresh` — re-curate existing themes

1. Load themes from `data/themes.yaml`
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

`data/themes.yaml` is the source of truth, committed to the repo. Review diffs after expand runs.

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

```bash
# Validate local setup (no API calls)
uv run python scripts/validate_exploration_themes_setup.py

# Create Typesense collection (requires TYPESENSE_* env vars)
uv run python scripts/create_exploration_themes_collection.py

# Expand with new themes
uv run python -m pipelines.exploration_themes.main_adhoc --mode expand --expand-count 30

# Refresh all themes
uv run python -m pipelines.exploration_themes.main_adhoc --mode refresh

# Refresh one theme
uv run python -m pipelines.exploration_themes.main_adhoc --mode refresh --theme-ids beach-day-with-friends

# QA UI
uv run streamlit run tools/streamlit_themes/app.py
```

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `AZURE_OPENAI_*` | — | LLM agents |
| `TYPESENSE_*` | — | Search + write |
| `EMBEDDINGS_SERVICE_URL` | — | Theme embeddings |
| `EXPLORATION_THEMES_COLLECTION` | `exploration_themes` | Typesense collection |
| `THEMES_FILE` | `data/themes.yaml` | Local theme store |

## Operational notes

- v1 runs **manually** via adhoc CLI; Azure timer planned post-QA
- `data/themes.yaml` is version-controlled; no blob storage for themes
- Also-buy daily pipeline is unaffected (separate entry point)
