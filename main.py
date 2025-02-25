import requests
import pandas as pd
import yfinance as yf
import time

# API Key di OpenFIGI (registrati gratuitamente su https://www.openfigi.com/)
OPENFIGI_API_KEY = "55360b21-1bb3-448b-9786-9ec77f8e1e62"


# Funzione per convertire ISIN in ticker utilizzando OpenFIGI API
def get_ticker_from_isin(isin):
    headers = {"Content-Type": "application/json"}
    if OPENFIGI_API_KEY:
        headers["X-OPENFIGI-APIKEY"] = OPENFIGI_API_KEY
    payload = [{"idType": "ID_ISIN", "idValue": isin}]
    url = "https://api.openfigi.com/v3/mapping"
    try:
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        if response.status_code == 200 and response_data[0].get("data"):
            ticker = response_data[0]["data"][0]["ticker"]
            print(f"🔍 ISIN: {isin} → Ticker trovato: {ticker}")
            return ticker
        else:
            print(f"⚠️ Nessun ticker trovato per ISIN: {isin}")
            return None
    except Exception as e:
        print(f"Errore nella richiesta OpenFIGI: {e}")
        return None


# Funzione per scaricare i dati storici di un titolo da Yahoo Finance
def fetch_stock_data(ticker, isin, start_date, end_date):
    try:
        print(f"📡 Scaricamento dati per {ticker} ({isin}) da {start_date} a {end_date}")
        stock_data = yf.download(ticker, start=start_date, end=end_date)
        if stock_data.empty:
            print(f"⚠️ Nessun dato trovato per {isin}")
            return None
        stock_data = stock_data[['Open', 'High', 'Low', 'Close']]
        stock_data.reset_index(inplace=True)

        # Aggiungiamo il ticker come prima colonna
        result_df = pd.DataFrame()
        result_df['Ticker'] = [ticker] * len(stock_data)
        result_df['ISIN'] = [isin] * len(stock_data)
        result_df['Date'] = stock_data['Date'].dt.strftime('%d/%m/%Y')

        # Stampiamo i dati per debug
        print(f"📊 Dati grezzi per {ticker} ({isin}):\n", stock_data.tail())

        # Convertire i numeri in formato con virgola come separatore decimale
        for col in ['Open', 'High', 'Low', 'Close']:
            result_df[col] = stock_data[col].map(lambda x: f"{x:.2f}".replace('.', ','))

        return result_df
    except Exception as e:
        print(f"Errore nel download dei dati per {isin}: {e}")
        return None


# Lettura della lista di ISIN dal file CSV
def process_isin_file(input_csv, output_csv, start_date, end_date):
    df = pd.read_csv(input_csv, sep=";")
    if 'ISIN' not in df.columns:
        print("⚠️ Il file CSV deve contenere una colonna 'ISIN'")
        return
    all_data = []
    for isin in df['ISIN'].unique():
        print(f"🔍 Elaborazione ISIN: {isin}")
        ticker = get_ticker_from_isin(isin)
        if ticker:
            time.sleep(1)  # Evita di superare i limiti API
            stock_data = fetch_stock_data(ticker, isin, start_date, end_date)
            if stock_data is not None:
                all_data.append(stock_data)
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Non usiamo dropna perché le colonne potrebbero essere stringhe con virgole
        final_df.to_csv(output_csv, index=False, sep=";", encoding="utf-8")
        print(f"✅ Dati salvati in {output_csv}")
    else:
        print("❌ Nessun dato disponibile per gli ISIN forniti.")


# Esegui lo script
if __name__ == "__main__":
    input_csv = "isin_list.csv"  # File con gli ISIN (separato da ;)
    output_csv = "stock_data.csv"
    start_date = "2024-09-01"
    end_date = "2024-09-30"
    process_isin_file(input_csv, output_csv, start_date, end_date)