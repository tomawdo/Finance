import pandas as pd
import yfinance as yf
from datetime import datetime
import time


def get_stock_data(symbol, date):
    """
    Recupera i dati di una singola azione per una data specifica
    """
    try:
        # Converti la data in oggetto datetime se non lo è già
        if isinstance(date, str):
            date = datetime.strptime(date, "%d/%m/%Y")

        # Scarica i dati per l'intero mese contenente la data di interesse
        start_date = date.replace(day=1)
        end_date = date.replace(day=28)  # Usiamo il 28 per sicurezza con febbraio

        # Scarica i dati
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)

        # Se non ci sono dati, restituisci None
        if data.empty:
            return None

        # Prendi i dati per la data specifica
        date_str = date.strftime("%Y-%m-%d")
        if date_str in data.index:
            day_data = data.loc[date_str]
            return {
                'ID SHARE': symbol,
                'Trade Date': date.strftime("%d/%m/%Y"),
                'Open': round(day_data['Open'].values[0], 2),
                'Close': round(day_data['Close'].values[0], 2),
                'High': round(day_data['High'].values[0], 2),
                'Low': round(day_data['Low'].values[0], 2)
            }
        return None

    except Exception as e:
        print(f"Errore nel recupero dei dati per {symbol} alla data {date}: {str(e)}")
        return None


def process_stock_data(input_file, output_file):
    """
    Elabora il file CSV di input e genera il file di output con i dati delle azioni
    """
    try:
        # Leggi il file CSV di input
        df_input = pd.read_csv(input_file)

        # Lista per memorizzare i risultati
        results = []

        # Elabora ogni riga del file di input
        total_rows = len(df_input)
        for idx, row in df_input.iterrows():
            # Mostra progresso
            print(f"Elaborazione {idx + 1}/{total_rows}: {row['ID SHARE']}")

            # Recupera i dati
            stock_data = get_stock_data(row['ID SHARE'], row['Trade Date'])

            if stock_data:
                results.append(stock_data)

            # Aggiungi un piccolo ritardo per evitare di sovraccaricare l'API
            time.sleep(1)

        # Crea il DataFrame dei risultati
        df_output = pd.DataFrame(results)

        # Salva i risultati in un nuovo file CSV
        df_output.to_csv(output_file, index=False)
        print(f"\nElaborazione completata. I risultati sono stati salvati in {output_file}")

    except Exception as e:
        print(f"Errore durante l'elaborazione del file: {str(e)}")


if __name__ == "__main__":
    # Nomi dei file di input e output
    input_file = "input_symbols.csv"  # Il tuo file CSV con ID SHARE e Trade Date
    output_file = "output_stock_data.csv"  # Il file che verrà generato

    # Esegui l'elaborazione
    process_stock_data(input_file, output_file)