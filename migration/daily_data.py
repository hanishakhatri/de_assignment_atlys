import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

# API details for Alpha Vantage
API_ENDPOINT = "https://www.alphavantage.co/query"
API_KEY = "DO327ZRO3X3YKEWG"

# Top 10 Indian companies by market cap (BSE tickers)
COMPANIES = ["RELIANCE.BSE", "TCS.BSE", "HDFCBANK.BSE", "ICICIBANK.BSE", "BHARTIARTL.BSE", "SBIN.BSE", "INFY.BSE", "LICI.BSE", "HINDUNILVR.BSE", "ITC.BSE"]

# Database connection parameters
DB_PARAMS = {
    "dbname": "stock_market_data",
    "user": "postgres",
    "password": "Hanisha@123",
    "host": "localhost",
    "port": "5432"
}

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

def daily_data(symbol, yesterday_date):
    """
    Fetches yesterday stock data for a given symbol from Alpha Vantage API,
    filters it based on the date range, and returns it as a pandas DataFrame.
    """
    try:
        data = request(symbol)

        if data is None:
            raise ValueError("Error fetching data. API request failed or returned invalid data.")

        if "Time Series (Daily)" not in data:
            note_message = data.get('Note', 'Exceeded API rate limit of 25 requests per minute.')
            raise ValueError(f"Error fetching data for {symbol}: {note_message}")

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

        # Filter the DataFrame for yesterday's date
        filtered_df = df[df['Date'] == yesterday_date]

        if filtered_df.empty:
            raise ValueError(f"No data available for {symbol} on {yesterday_date}")

        return filtered_df
    
    except Exception as e:
        raise e


def connect_to_db():
    """
    Establishes a connection to the database and returns the connection object.
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Database connection error: {e}")
        raise e

def create_table(conn):
    """
    Creates the stock_data table if it does not exist.
    """
    try:
        with conn.cursor() as cur:
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
            cur.execute(create_table_query)
            conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Error creating table: {e}")
        raise e

def insert_data(conn, data):
    """
    Inserts data into the stock_data table. Updates existing records if there's a conflict.
    """
    try:
        with conn.cursor() as cur:
            insert_query = sql.SQL("""
            INSERT INTO stock_data (Date, Company, Open, High, Low, Close, Volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Date, Company) DO UPDATE SET
                Open = EXCLUDED.Open,
                High = EXCLUDED.High,
                Low = EXCLUDED.Low,
                Close = EXCLUDED.Close,
                Volume = EXCLUDED.Volume;
            """)
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
                cur.execute(insert_query, values)
            conn.commit()
    except psycopg2.DatabaseError as e:
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
    except psycopg2.DatabaseError as e:
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
    try:
        yesterday_date = (datetime.now() - timedelta(days=1)).date()
        yesterday_date = yesterday_date.strftime('%Y-%m-%d')
        for company in COMPANIES:
            print(f"Fetching data for {company}")
            df = daily_data(company,yesterday_date)
            if df:
                data = convert_data_type(df)
                insert_data_to_db(data)
                print(f"data succesfully inserted for {company}")
    except Exception as e:
        raise e 

if __name__ == "__main__":
    main()
