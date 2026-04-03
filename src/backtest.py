from __future__ import annotations

import pandas as pd


def run_backtest(
    df: pd.DataFrame,
    tp_pct: float = 0.01,   # take profit: %1
    sl_pct: float = 0.005,  # stop loss:   %0.5
    max_bars: int = 50,     # maksimum kaç mum bekleyelim
) -> pd.DataFrame:
    """
    signal=1 olan her mum için basit bir TP/SL backtesti çalıştırır.

    Parametreler:
        tp_pct   : kazanç hedefi (örn: 0.01 → %1 yukarı)
        sl_pct   : zarar kesme  (örn: 0.005 → %0.5 aşağı)
        max_bars : bu kadar mum içinde sonuç çıkmazsa işlemi "zaman aşımı" say

    Döndürür:
        Her işlemi bir satır olarak içeren DataFrame
    """

    results = []  # her işlemin sonucunu buraya ekleyeceğiz

    # signal=1 olan satırların index numaralarını bul
    signal_indices = df.index[df["signal"] == 1].tolist()

    for entry_idx in signal_indices:

        # Giriş fiyatı: sinyal mumunun kapanış fiyatı
        entry_price = df.loc[entry_idx, "close"]
        entry_time  = df.loc[entry_idx, "open_time"]

        # Hedef ve zarar seviyelerini hesapla
        take_profit = entry_price * (1 + tp_pct)
        stop_loss   = entry_price * (1 - sl_pct)

        outcome = "timeout"  # varsayılan: ne TP ne SL tetiklenmedi

        # Giriş mumundan sonraki mumlara tek tek bak
        for i in range(1, max_bars + 1):
            next_idx = entry_idx + i

            # Veri bitti mi? (son mumlarda bu olabilir)
            if next_idx >= len(df):
                break

            high = df.loc[next_idx, "high"]
            low  = df.loc[next_idx, "low"]

            # Önce TP mi geldi, yoksa SL mi?
            # NOT: Aynı mumda ikisi de olabilir — hangisi önce geldi bilemeyiz.
            # Basitlik için: önce TP'yi kontrol ediyoruz (iyimser yaklaşım).
            if high >= take_profit:
                outcome = "win"
                break
            if low <= stop_loss:
                outcome = "loss"
                break

        results.append({
            "entry_time":   entry_time,
            "entry_idx":    entry_idx,
            "entry_price":  entry_price,
            "take_profit":  take_profit,
            "stop_loss":    stop_loss,
            "outcome":      outcome,
        })

    return pd.DataFrame(results)
