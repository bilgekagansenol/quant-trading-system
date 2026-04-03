from __future__ import annotations

import numpy as np
import pandas as pd


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["return"] = df["close"].pct_change() #pct_change  şimdiki ile önceki element arasındaki farkı oranlayarak verir.  100 ile çarğılırsa yüzdelik değeri de alınabilir.

    df["log_return"] = np.log(df["close"] / df["close"].shift(1)) # burdsa shift ile 1 önceki elementin değerini alıyor.

    return df


def add_ema(df: pd.DataFrame, span: int = 20, col_name: str | None = None) -> pd.DataFrame:
    df = df.copy()
    if col_name is None:
        col_name = f"ema_{span}"
    df[col_name] = df["close"].ewm(span=span, adjust=False).mean()  ### BURDA KALDIK
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    df = df.copy()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df[f"rsi_{period}"] = 100 - (100 / (1 + rs))

    return df


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    df = df.copy()

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()

    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df[f"atr_{period}"] = true_range.ewm(alpha=1 / period, adjust=False).mean()

    return df


def add_volatility(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    df = df.copy()
    if "return" not in df.columns:
        df["return"] = df["close"].pct_change()

    df[f"volatility_{window}"] = df["return"].rolling(window).std()
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = add_returns(df)
    df = add_ema(df, span=20)
    df = add_ema(df, span=50)
    df = add_rsi(df, period=14)
    df = add_atr(df, period=14)
    df = add_volatility(df, window=20)

    return df