"""Shared reference data for Veloz's Faker-based generators.

`orders.py` and `fulfillment.py` both need the same dark-store, rider and SKU
dimensions so that downstream joins (e.g. an order's `store_id` matching a row
in the fulfillment feed) make sense. Rather than a database or an on-disk
fixture, this module builds those dimensions deterministically at import time:
every process that imports it gets the identical store/rider/SKU lists.

The seed here is fixed and independent of each generator's own `--seed` flag —
that flag controls the random *content* a generator produces (which orders,
which stock counts), not the *dimensions* both generators must agree on.
"""

from __future__ import annotations

from dataclasses import dataclass

from faker import Faker

# Fixed on purpose: this seed controls only the reference dimensions (stores,
# riders, SKUs), which must be identical across every run and every process
# that imports this module — unlike each generator's own --seed, which is
# meant to vary run-to-run.
_REFERENCE_SEED = 20240115

CITIES = ["Medellín", "Bogotá", "São Paulo"]
NUM_STORES = 30
NUM_RIDERS = 450


@dataclass(frozen=True)
class Store:
    """A Veloz dark store."""

    store_id: str
    city: str
    name: str


@dataclass(frozen=True)
class Sku:
    """A catalog item stocked by dark stores."""

    sku: str
    name: str
    category: str


_SKU_CATALOG_BY_CATEGORY = {
    "Produce": ["Banana", "Avocado", "Tomato", "Onion", "Potato", "Lettuce", "Lime", "Mango"],
    "Dairy": ["Whole Milk 1L", "Greek Yogurt", "Cheddar Cheese", "Butter", "Eggs (12ct)"],
    "Bakery": ["White Bread", "Whole Wheat Bread", "Croissant", "Bagel (4ct)"],
    "Snacks": ["Potato Chips", "Chocolate Bar", "Trail Mix", "Granola Bar (6ct)"],
    "Beverages": ["Sparkling Water 1L", "Orange Juice 1L", "Cola 2L", "Coffee 340g"],
    "Household": ["Paper Towels (2ct)", "Dish Soap", "Trash Bags (20ct)", "Laundry Detergent"],
    "Personal Care": ["Toothpaste", "Shampoo", "Hand Soap", "Toilet Paper (4ct)"],
}


def _build_stores(fake: Faker) -> list[Store]:
    """Distributes NUM_STORES stores round-robin across CITIES."""
    stores = []
    for i in range(1, NUM_STORES + 1):
        city = CITIES[(i - 1) % len(CITIES)]
        store_id = f"STORE-{i:03d}"
        name = f"Veloz {city} - {fake.street_name()}"
        stores.append(Store(store_id=store_id, city=city, name=name))
    return stores


def _build_riders(fake: Faker) -> dict[str, str]:
    """Assigns each rider_id to one home city, evenly split across cities.

    Riders work within a single city, so orders.py can assign a rider to an
    order using the order's own store city rather than picking from all 450.
    """
    rider_city: dict[str, str] = {}
    riders_per_city = NUM_RIDERS // len(CITIES)
    rider_num = 1
    for city in CITIES:
        for _ in range(riders_per_city):
            rider_city[f"RIDER-{rider_num:04d}"] = city
            rider_num += 1
    # Any remainder from integer division goes to the last city.
    while rider_num <= NUM_RIDERS:
        rider_city[f"RIDER-{rider_num:04d}"] = CITIES[-1]
        rider_num += 1
    return rider_city


def _build_skus() -> list[Sku]:
    skus = []
    counter = 1
    for category, items in _SKU_CATALOG_BY_CATEGORY.items():
        for item_name in items:
            skus.append(Sku(sku=f"SKU-{counter:04d}", name=item_name, category=category))
            counter += 1
    return skus


_fake = Faker()
_fake.seed_instance(_REFERENCE_SEED)

STORES: list[Store] = _build_stores(_fake)
STORE_IDS: list[str] = [s.store_id for s in STORES]
STORE_CITY: dict[str, str] = {s.store_id: s.city for s in STORES}

RIDER_CITY: dict[str, str] = _build_riders(_fake)
RIDER_IDS: list[str] = list(RIDER_CITY.keys())
CITY_RIDERS: dict[str, list[str]] = {
    city: [r for r, c in RIDER_CITY.items() if c == city] for city in CITIES
}

SKUS: list[Sku] = _build_skus()
SKU_IDS: list[str] = [s.sku for s in SKUS]


def riders_for_store(store_id: str) -> list[str]:
    """Returns the riders eligible to serve a given store (same city)."""
    return CITY_RIDERS[STORE_CITY[store_id]]
