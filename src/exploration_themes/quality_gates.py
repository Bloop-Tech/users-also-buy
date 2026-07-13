from __future__ import annotations

from src.exploration_themes.data_models import ExplorationTheme

MIN_PRODUCTS = 8
MAX_PRODUCTS = 15
MAX_PER_BRAND = 3


def apply_quality_gates(
    theme: ExplorationTheme,
    *,
    selected_product_ids: list[str],
    candidate_ids: set[str],
    candidate_brands: dict[str, str | None],
) -> ExplorationTheme:
    valid_ids = [pid for pid in selected_product_ids if pid in candidate_ids]
    valid_ids = _enforce_brand_diversity(valid_ids, candidate_brands)
    valid_ids = valid_ids[:MAX_PRODUCTS]

    theme.product_ids = valid_ids
    theme.version += 1

    if len(valid_ids) >= MIN_PRODUCTS:
        theme.status = "active"
    else:
        theme.status = "insufficient_products"
    return theme


def _enforce_brand_diversity(
    product_ids: list[str],
    candidate_brands: dict[str, str | None],
) -> list[str]:
    brand_counts: dict[str, int] = {}
    result: list[str] = []
    for product_id in product_ids:
        brand = (candidate_brands.get(product_id) or "unknown").lower()
        count = brand_counts.get(brand, 0)
        if count >= MAX_PER_BRAND:
            continue
        brand_counts[brand] = count + 1
        result.append(product_id)
    return result


def should_publish_to_typesense(theme: ExplorationTheme) -> bool:
    return theme.status == "active" and len(theme.product_ids) >= MIN_PRODUCTS
