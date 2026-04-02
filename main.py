from src.data_loader import fetch_historical_klines ,save_dataframe

def main() -> None:
    df = fetch_historical_klines(
        symbol="BTCUSDT",
        interval="5m",
        total_limit=5000,  # ilk deneme için yeterli
    )

    print(df.head())
    print(df.tail())
    print(df.shape)

    save_dataframe(
        df,
        csv_path="data/raw/btcusdt_5m.csv",
        parquet_path="data/raw/btcusdt_5m.parquet",
    )

    print("Veri başarıyla kaydedildi.")


if __name__ == "__main__":  # entry point control olarak geçer 
    main()



    