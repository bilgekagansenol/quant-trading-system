from src.data_loader import fetch_historical_klines, save_dataframe
from src.features import build_features
from src.strategy import add_basic_signal
from src.backtest import run_backtest


def main() -> None:
    # 1. veri çek
    df = fetch_historical_klines(
        symbol="BTCUSDT",
        interval="5m",
        total_limit=5000,
    )

    # 2. feature üret
    df = build_features(df)

    # 3. strateji — sinyal üret
    df = add_basic_signal(df)

    # 4. backtest çalıştır
    trades = run_backtest(df, tp_pct=0.01, sl_pct=0.005)

    # 5. sonuçları göster
    total   = len(trades)
    wins    = (trades["outcome"] == "win").sum()
    losses  = (trades["outcome"] == "loss").sum()
    timeouts = (trades["outcome"] == "timeout").sum()
    win_rate = wins / total * 100 if total > 0 else 0

    print(f"\n--- Backtest Sonuçları ---")
    print(f"Toplam işlem : {total}")
    print(f"Kazanan      : {wins}")
    print(f"Kaybeden     : {losses}")
    print(f"Zaman aşımı  : {timeouts}")
    print(f"Win rate     : %{win_rate:.1f}")

    # 6. kaydet
    save_dataframe(
        df,
        csv_path="data/processed/btcusdt_5m_features.csv",
        parquet_path="data/processed/btcusdt_5m_features.parquet",
    )

    print("\nHer şey tamam.")


if __name__ == "__main__":
    main()