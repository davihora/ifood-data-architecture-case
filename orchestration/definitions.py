"""Dagster entrypoint: `dagster dev -m orchestration.definitions`."""

from __future__ import annotations

import dagster as dg

from orchestration.assets import bronze_trips, gold_marts, landing, silver_trips

defs = dg.Definitions(assets=[landing, bronze_trips, silver_trips, gold_marts])
