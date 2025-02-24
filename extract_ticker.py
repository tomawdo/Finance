import pandas as pd
import requests
import json

def get_ticker_from_isin(isin):
    """
    Recupera il ticker di un'azione a partire dal suo codice ISIN usando OpenFIGI API
    """
    try:
        url = "https://api.openfigi.com/v3/mapping"
        headers = {"Content-Type": "application/json"}
        data = {"query": [{"idType": "ID_ISIN", "idValue": isin}]}

        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            result = response.json()
            if result and 'data' in result and result['data']:
                return result['data'][0]['ticker']
        return None
    except Exception as e:
        print(f"Errore durante il recupero del ticker per ISIN {isin}: {str(e)}")
        return None

def process_isin_data(input_file, output_file):
    """
    Elabora il file CSV di input e genera il file di output con i ticker
    """
    try:
        # Leggi il file CSV di input
        df_input = pd.read_csv(input_file)

        # Aggiungi una colonna per i ticker
        df_input['Ticker'] = df_input['ISIN'].apply(get_ticker_from_isin)

        # Salva i risultati in un nuovo file CSV
        df_input.to_csv(output_file, index=False)
        print(f"\nElaborazione completata. I risultati sono stati salvati in {output_file}")

    except Exception as e:
        print(f"Errore durante l'elaborazione del file: {str(e)}")

if __name__ == "__main__":
    # Nomi dei file di input e output
    input_file = "input_isin.csv"  # Il tuo file CSV con la colonna ISIN
    output_file = "output_isin_ticker.csv"  # Il file che verrà generato

    # Esegui l'elaborazione
    process_isin_data(input_file, output_file)