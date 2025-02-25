import csv
import requests
from bs4 import BeautifulSoup

def get_ticker(isin):
    url = f"https://www.google.com/finance/quote/{isin}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    ticker_elem = soup.select_one('div[data-last-price]')
    if ticker_elem:
        return ticker_elem["data-symbol"]
    return "Not Found"

# Read ISIN from CSV
with open("isin.csv", "r") as file:
    reader = csv.reader(file)
    isins = [row[0] for row in reader]

# Get Ticker for each ISIN
data = []
for isin in isins:
    ticker = get_ticker(isin)
    data.append([isin, ticker])

# Write data to new CSV
with open("isin_ticker.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["ISIN", "Ticker"])
    writer.writerows(data)

print("isin_ticker.csv generated successfully.")