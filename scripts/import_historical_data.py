"""
Idempotent Historical Data Importer for PricePulse.
Imports product metadata and time-series price history records from JSON.

Maintains data integrity:
- Preserves existing records; avoids duplicate products and price observations.
- Uses transactions to ensure atomicity.
- Calculates accurate all-time low, high, and latest price aggregates.
- Integrates with search indexing if available.
"""

import os
import sys
import json
import uuid
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

# Ensure app is importable
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
os.chdir(str(BACKEND_DIR))

from sqlalchemy import select, func
from app.config import settings
from app.database import AsyncSessionLocal, init_db
from app.models.product import Product
from app.models.price_history import PriceHistory
from app.services.search_service import search_service


def parse_iso_datetime(dt_str: str) -> datetime:
    """Safely parse ISO datetime string to UTC-aware datetime."""
    if not dt_str:
        return datetime.now(timezone.utc)
    cleaned = dt_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def ensure_utc(dt: Any) -> datetime:
    """Ensure datetime is UTC-aware even when loaded from SQLite."""
    if dt is None:
        return datetime.now(timezone.utc)
    if isinstance(dt, str):
        return parse_iso_datetime(dt)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def import_historical_data(json_file_path: str) -> Dict[str, Any]:
    file_path = Path(json_file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Source JSON file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    prod_raw = data.get("product")
    if not prod_raw:
        raise ValueError("JSON file must contain a 'product' object at root.")

    price_history_raw: List[Dict[str, Any]] = data.get("price_history", [])
    if not price_history_raw:
        raise ValueError("JSON file must contain 'price_history' records.")

    print(f"[*] Loaded JSON: {file_path.name}")
    print(f"    Product Title: {prod_raw.get('title')}")
    print(f"    Raw Records: {len(price_history_raw)}")

    # Ensure tables exist
    await init_db()

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # 1. Check if product already exists (by ID or canonical_key)
            prod_id = prod_raw.get("id") or str(uuid.uuid4())
            canonical_key = prod_raw.get("canonical_key")

            stmt = select(Product).where(
                (Product.id == prod_id) | (Product.canonical_key == canonical_key)
            )
            res = await session.execute(stmt)
            product = res.scalar_one_or_none()

            is_new_product = False
            first_seen_at = parse_iso_datetime(prod_raw.get("first_seen_at"))
            last_observed_at = parse_iso_datetime(prod_raw.get("last_observed_at"))

            if not product:
                is_new_product = True
                product = Product(
                    id=prod_id,
                    canonical_key=canonical_key or f"{prod_raw.get('platform', 'amazon')}:US:{prod_id[:8]}",
                    platform=prod_raw.get("platform", "amazon"),
                    url=prod_raw.get("url", ""),
                    title=prod_raw.get("title", ""),
                    image_url=prod_raw.get("image_url"),
                    currency=prod_raw.get("currency", "USD"),
                    region=prod_raw.get("region", "US"),
                    category=prod_raw.get("category"),
                    first_seen_at=first_seen_at,
                    poll_interval_s=prod_raw.get("poll_interval_s", 21600),
                    is_dormant=prod_raw.get("is_dormant", False),
                    last_price=prod_raw.get("last_price"),
                    all_time_low=prod_raw.get("all_time_low"),
                    all_time_high=prod_raw.get("all_time_high"),
                    last_observed_at=last_observed_at,
                )
                session.add(product)
                await session.flush()
                print(f"[+] Created product record: {product.title} (ID: {product.id})")
            else:
                print(f"[i] Product exists (ID: {product.id}). Updating metadata...")
                if prod_raw.get("title"):
                    product.title = prod_raw["title"]
                if prod_raw.get("image_url"):
                    product.image_url = prod_raw["image_url"]
                if prod_raw.get("category"):
                    product.category = prod_raw["category"]

            target_product_id = product.id

            # 2. Query existing price history for target_product_id to prevent duplicates
            hist_stmt = select(PriceHistory.id, PriceHistory.observed_at).where(
                PriceHistory.product_id == target_product_id
            )
            hist_res = await session.execute(hist_stmt)
            existing_records = hist_res.all()
            existing_ids = {r[0] for r in existing_records}
            existing_timestamps = {
                ensure_utc(r[1]).isoformat() for r in existing_records
            }

            added_count = 0
            skipped_count = 0
            all_prices: List[float] = []
            latest_time: datetime = ensure_utc(product.first_seen_at)

            for item in price_history_raw:
                rec_id = item.get("id") or str(uuid.uuid4())
                dt = parse_iso_datetime(item.get("observed_at"))
                dt_iso = dt.isoformat()
                price_val = float(item.get("price", 0.0))

                if price_val <= 0:
                    continue  # Skip invalid price records

                # Duplicate prevention: check both record id and timestamp for this product
                if rec_id in existing_ids or dt_iso in existing_timestamps:
                    skipped_count += 1
                    all_prices.append(price_val)
                    if dt > latest_time:
                        latest_time = dt
                    continue

                rec = PriceHistory(
                    id=rec_id,
                    product_id=target_product_id,
                    observed_at=dt,
                    price=price_val,
                    currency=item.get("currency", product.currency),
                    availability=item.get("availability", True),
                    source_run_id=item.get("source_run_id") or str(uuid.uuid4()),
                )
                session.add(rec)
                added_count += 1
                all_prices.append(price_val)
                if dt > latest_time:
                    latest_time = dt

            await session.flush()

            # 3. Recalculate true aggregate stats from actual observations
            stats_stmt = select(
                func.min(PriceHistory.price),
                func.max(PriceHistory.price),
                func.count(PriceHistory.id),
            ).where(PriceHistory.product_id == target_product_id)
            stats_res = await session.execute(stats_stmt)
            min_price, max_price, total_obs = stats_res.first()

            # Find latest price
            latest_rec_stmt = (
                select(PriceHistory.price, PriceHistory.observed_at)
                .where(PriceHistory.product_id == target_product_id)
                .order_by(PriceHistory.observed_at.desc())
                .limit(1)
            )
            latest_res = await session.execute(latest_rec_stmt)
            latest_row = latest_res.first()
            if latest_row:
                product.last_price = float(latest_row[0])
                product.last_observed_at = latest_row[1]
            if min_price is not None:
                product.all_time_low = float(min_price)
            if max_price is not None:
                product.all_time_high = float(max_price)

            print(f"[+] Price History: {added_count} records inserted, {skipped_count} existing skipped.")
            print(f"[+] Total Observations in DB: {total_obs}")
            print(f"    Current Price: ${product.last_price:.2f}")
            print(f"    All-Time Low:  ${product.all_time_low:.2f}")
            print(f"    All-Time High: ${product.all_time_high:.2f}")

        # Transaction commits automatically here on exiting session.begin()

    # 4. Update Meilisearch search index (graceful fallback)
    try:
        search_service.index_product(product)
        print("[+] Product indexed into search engine.")
    except Exception as e:
        print(f"[i] Search indexing skipped: {e}")

    return {
        "product_id": target_product_id,
        "is_new": is_new_product,
        "records_added": added_count,
        "records_skipped": skipped_count,
        "total_observations": total_obs,
        "current_price": float(product.last_price or 0.0),
        "all_time_low": float(product.all_time_low or 0.0),
        "all_time_high": float(product.all_time_high or 0.0),
    }


def main():
    parser = argparse.ArgumentParser(description="Import historical product price data into PricePulse.")
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        default=str(BACKEND_DIR / "data" / "product_price_history_1year.json"),
        help="Path to product price history JSON file.",
    )
    args = parser.parse_args()

    result = asyncio.run(import_historical_data(args.file))
    print("\n[SUCCESS] Import completed successfully!")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
