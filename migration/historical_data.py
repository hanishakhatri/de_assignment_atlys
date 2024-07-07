import requests
import pandas as pd
from datetime import datetime
import sqlite3

# API details for Alpha Vantage
API_ENDPOINT = "https://www.alphavantage.co/query"
API_KEY = "JEM10FUVKPMSXGQO"
DB_PARAMS = {
    "dbname": "stock_data.db"
}


# Top 10 Indian companies by market cap (BSE tickers)
COMPANIES = ["RELIANCE.BSE", "TCS.BSE", "HDFCBANK.BSE", "ICICIBANK.BSE", "BHARTIARTL.BSE", "SBIN.BSE", "INFY.BSE", "LICI.BSE", "HINDUNILVR.BSE", "ITC.BSE"]

# Database connection parameters


def request(symbol):
    """ Request to the alpha vinatge API """
    try :
        params = {
            "function": "TIME_SERIES_DAILY",
            "apikey": API_KEY,
            "outputsize": "full",
            "symbol": symbol
        }
        response = requests.get(API_ENDPOINT, params=params)
        data = response.json()
        return data
    except Exception as e :
        return e

def fetch_historical_data(symbol, start_date, end_date):
    """
    Fetches historical stock data for a given symbol from Alpha Vantage API,
    filters it based on the date range, and returns it as a pandas DataFrame.
    """
    try:
        data = request(symbol)

        if data is None:
            print("Error fetching data. API request failed or returned invalid data.")

        if "Time Series (Daily)" in data:
            formatted_data = []
            for date, values in data["Time Series (Daily)"].items():
                formatted_data.append({
                    "Date": datetime.strptime(date, '%Y-%m-%d'),
                    "Company": symbol,
                    "Open": float(values["1. open"]),
                    "High": float(values["2. high"]),
                    "Low": float(values["3. low"]),
                    "Close": float(values["4. close"]),
                    "Volume": int(values["5. volume"])
                })

            df = pd.DataFrame(formatted_data)

            # Filter the DataFrame from start date to end date 
            filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

            if filtered_df.empty:
                print(f"No data available for {symbol}")

            return filtered_df
        else:
            note_message = data.get('Note', 'Exceeded API rate limit of 25 requests per minute.')
            print(f"Error fetching data for {symbol}: {note_message}")
    
    except Exception as e:
        raise e
    

def connect_to_db():
    """
    Establishes a connection to the database and returns the connection object.
    """
    try:
        conn = sqlite3.connect(DB_PARAMS["dbname"])
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise e

def create_table(conn):
    """
    Creates the stock_data table if it does not exist.
    """
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            Date DATE,
            Company VARCHAR(20),
            Open NUMERIC,
            High NUMERIC,
            Low NUMERIC,
            Close NUMERIC,
            Volume BIGINT,
            PRIMARY KEY (Date, Company)
        );
        """
        conn.execute(create_table_query)
        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Error creating table: {e}")
        raise e

def insert_data(conn, data):
    """
    Inserts data into the stock_data table. Updates existing records if there's a conflict.
    """
    try:
        insert_query = """
        INSERT INTO stock_data (Date, Company, Open, High, Low, Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(Date, Company) DO UPDATE SET
            Open = excluded.Open,
            High = excluded.High,
            Low = excluded.Low,
            Close = excluded.Close,
            Volume = excluded.Volume;
        """

        with conn:
            for row in data:
                values = (
                    row["Date"],
                    row["Company"],
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"]
                )
                conn.execute(insert_query, values)
            conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
        raise e

def insert_data_to_db(data):
    """
    Main function to handle the entire process of connecting to the database, 
    creating the table, and inserting the data.
    """
    try:
        conn = connect_to_db()
        create_table(conn)
        insert_data(conn, data)
    except sqlite3.DatabaseError as e:
        print(f"Database error occurred: {e}")
        raise e
    finally:
        if conn:
            conn.close()
        
def convert_data_type(df):
    # Convert data types
    try:
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Company'] = df['Company'].astype(str)
        df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
        df['High'] = pd.to_numeric(df['High'], errors='coerce')
        df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').astype('Int64')  # Using 'Int64' to handle NaNs

        # Convert DataFrame to list of dictionaries
        list_of_dicts = df.to_dict(orient='records')
        return list_of_dicts
    except Exception as e:
        raise e 

def main():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 5, 31)
    try:
        for company in COMPANIES:
            print(f"Fetching data for {company}")
            df = fetch_historical_data(company, start_date, end_date)
            if df is not None:
                data = convert_data_type(df)
                insert_data_to_db(data)
                print(f"data succesfully inserted for {company}")
    except Exception as e:
        raise e 


if __name__ == "__main__":
    main()

