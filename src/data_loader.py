from __future__ import annotations

import time
from pathlib import Path
from typing import List

import pandas as pd
import requests


BASE_URL = "https://data-api.binance.vision"
KLINES_ENDPOINT = "/api/v3/klines"


def fetch_klines(
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    limit: int = 1000,
    start_time: int | None = None,
    end_time: int | None = None,
) -> List[list]:
    """
    Binance Spot API'den kline verisi çeker.

    Parametreler:
        symbol: İşlem çifti
        interval: Mum periyodu (örn: 5m, 1h)
        limit: Tek istekte maksimum satır sayısı
        start_time: milisecond cinsinden başlangıç zamanı
        end_time: milisecond cinsinden bitiş zamanı
    """
    url = f"{BASE_URL}{KLINES_ENDPOINT}"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    response = requests.get(url, params=params, timeout=10)# boşa beklemesin diye timeout attık, response olmazsa işlevine devam etmesi gerekiyor.

    response.raise_for_status()

    return response.json()


def klines_to_dataframe(klines: List[list]) -> pd.DataFrame:
    """
    Binance kline çıktısını pandas DataFrame'e çevirir.
    """
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    df = pd.DataFrame(klines, columns=columns)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    df = df.sort_values("open_time").reset_index(drop=True)  # burda open timea göre sıra oluşuyor ama normalde index değişmiyor bu sıralamada yeni oluşan sıraya göre index alması için reset_index atıyoruz drop true ile de eski indexin de tutulmasını engellemiş oluyoruz.

    return df


def fetch_historical_klines(
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    total_limit: int = 5000,
    pause_seconds: float = 0.2,
) -> pd.DataFrame:
    """
    Çoklu istek atarak daha fazla geçmiş veri çeker.
    total_limit yaklaşık istenen toplam satır sayısıdır.
    """
    all_rows: List[list] = []
    start_time = None

    while len(all_rows) < total_limit:
        remaining = total_limit - len(all_rows)
        batch_limit = min(1000, remaining)  # hangisi en düşükse batch olarak onu alalım diyor

        batch = fetch_klines(
            symbol=symbol,
            interval=interval,
            limit=batch_limit,
            start_time=start_time,
        )

        if not batch:
            break

        all_rows.extend(batch)

        last_open_time = batch[-1][0]
        start_time = last_open_time + 1

        time.sleep(pause_seconds)

        # Güvenlik: aynı veri tekrar gelirse döngü kır
        if len(batch) < batch_limit:
            break

    # open_time bazlı tekrarları sil
    df = klines_to_dataframe(all_rows)
    df = df.drop_duplicates(subset=["open_time"]).reset_index(drop=True)

    return df


def save_dataframe(
    df: pd.DataFrame,
    csv_path: str | Path,
    parquet_path: str | Path,   # str | Path   türünün 2sinden biri olması gerektiğini belirtmek için kullanıldı.
) -> None:
    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)   # index false pandasın keni ekstra  indexini yazmamak için kullanılır.