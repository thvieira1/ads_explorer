"""Synchronize the OLX extraction DataFrame to bronze.ads_information (SCD2)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


TABLE_SCHEMA = "bronze"
TABLE_NAME = "ads_information"
PAGE_SIZE = 1000


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


def _number(value: Any) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _price(record: dict[str, Any]) -> Decimal | None:
    price = _number(record.get("anuncio_price"))
    return price if price is not None else _number(record.get("preco_card"))


def _json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Use pandas' encoder to normalize NaN, NaT, timestamps and numpy scalars."""
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _read_all_rows(table: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        page = table.select(
            "anuncio_listId,anuncio_adId,anuncio_price,preco_card,"
            "dt_loadtime,flag_current,version"
        ).order("dt_loadtime").range(start, start + PAGE_SIZE - 1).execute().data or []
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows
        start += PAGE_SIZE


def sync_ads_dataframe(df_anuncios: pd.DataFrame, *, client: Any | None = None) -> dict[str, int]:
    """Insert new ads and price changes; leave unchanged ads untouched.

    Requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in the process environment
    or in a local .env file. Keep the service-role key on a trusted machine only.
    """
    load_dotenv()
    if client is None:
        client = create_client(_env("SUPABASE_URL"), _env("SUPABASE_SERVICE_ROLE_KEY"))

    if df_anuncios.empty:
        return {"novos": 0, "preco_alterado": 0, "sem_alteracao": 0, "ignorados": 0}

    df = df_anuncios.copy()
    if "erro_coleta" in df.columns:
        df = df[df["erro_coleta"].isna()]

    if "anuncio_listId" not in df.columns and "anuncio_adId" not in df.columns:
        raise ValueError("DataFrame não contém anuncio_listId nem anuncio_adId")
    df["_scd2_id"] = df.get("anuncio_listId", pd.Series(index=df.index, dtype="object"))
    if "anuncio_adId" in df.columns:
        df["_scd2_id"] = df["_scd2_id"].fillna(df["anuncio_adId"])
    df = df.dropna(subset=["_scd2_id"]).drop_duplicates(subset=["_scd2_id"], keep="last")
    df = df.drop(columns=["_scd2_id"])
    records = _json_records(df)

    # Query all versions once, so existing history determines the next version.
    table = client.schema(TABLE_SCHEMA).table(TABLE_NAME)
    existing_rows = _read_all_rows(table)
    histories: dict[str, list[dict[str, Any]]] = {}
    for row in existing_rows:
        ad_id = row.get("anuncio_listId") or row.get("anuncio_adId")
        if ad_id is not None:
            histories.setdefault(str(ad_id), []).append(row)

    result = {"novos": 0, "preco_alterado": 0, "sem_alteracao": 0, "ignorados": 0}
    for record in records:
        ad_id = record.get("anuncio_listId") or record.get("anuncio_adId")
        price = _price(record)
        if ad_id is None or price is None:
            result["ignorados"] += 1
            continue

        key = str(ad_id)
        history = histories.get(key, [])
        current = next((row for row in history if row.get("flag_current") is True), None)
        if current and _price(current) == price:
            result["sem_alteracao"] += 1
            continue

        next_version = max((int(row.get("version") or 0) for row in history), default=0) + 1
        current_id_column = None
        if current:
            current_id_column = "anuncio_listId" if current.get("anuncio_listId") is not None else "anuncio_adId"
            table.update({"flag_current": False}).eq(current_id_column, current[current_id_column]).eq(
                "version", current["version"]
            ).execute()

        record.update({
            "dt_loadtime": datetime.now(timezone.utc).isoformat(),
            "flag_current": True,
            "version": next_version,
        })
        try:
            table.insert(record).execute()
        except Exception:
            # Restore the prior current row if the replacement insert fails.
            # This is a best-effort compensation; PostgreSQL RPC is preferable
            # if strict transactionality or concurrent writers are required.
            if current and current_id_column:
                table.update({"flag_current": True}).eq(
                    current_id_column, current[current_id_column]
                ).eq("version", current["version"]).execute()
            raise

        result["preco_alterado" if current else "novos"] += 1
        histories.setdefault(key, []).append({
            "anuncio_listId": record.get("anuncio_listId"),
            "anuncio_adId": record.get("anuncio_adId"),
            "anuncio_price": record.get("anuncio_price"),
            "preco_card": record.get("preco_card"),
            "dt_loadtime": record["dt_loadtime"],
            "flag_current": True,
            "version": next_version,
        })

    return result
