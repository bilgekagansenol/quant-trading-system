from __future__ import annotations

import numpy as np
import pandas as pd


def add_basic_signal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    long_condition = (
        (df["close"] > df["ema_20"]) &
        (df["rsi_14"] > 55)
    )

    df["signal"] = np.where(long_condition, 1, 0)

    return df