# de_assignment_atlys
DE Assignment by Atlys

## TOP 10 MARKET CAP STOCK IN INDIA 
source link : https://www.forbesindia.com/article/explainers/top-10-companies-india-market-valuation/86307/1

Listed below are the top Indian companies by market cap: 

1.    Reliance Industries 
2.    Tata Consultancy Services (TCS)
3.    HDFC Bank 
4.    ICICI Bank 
5.    Bharti Airtel
6.    State Bank of India (SBI)
7.    Infosys 
8.    Life Insurance Corporation of India (LIC)
9.    Hindustan Unilever 
10.   ITC

# install all dependency
pip install -r requirements.txt

# install SqliteDB from the VScode extenstion 

# For storing historical stock data from Jan 1, 2020 to May 31, 2024 
## Run Command
python historical_data.py

# For Storing daily yesterday stock data (But the stock data is not update on the sunday like i did for 6 july 2024 but it shows empty)
## Run Command
python daily_data.py

# Querying the database so there is two ways

## Approach 1 It will save the data into an csv file with respective file name
1. Company Wise Daily Variation of Prices
### Run command
python daily_variation_of_prices.py 


2. Company Wise Daily Volume Change
### Run command
python daily_volume_change.py

3. Median Daily Variation
### Run command
python median_daily_variation.py  

## Approach 2
1. right click on the stock_data.db click on open database
2. In the bottom of the right side you can see SQLITE EXPLORE ->stock_data.db -> stock_data 
3. Click on stock_data then click on new query. 
4. Paste the select Query and run it.



<!-- API KEYS : DO327ZRO3X3YKEWG , M2PR1FF8ROD1EYM0,CT650XO3FHT3CW3P ,4AATNHC0FVZ905W7,JEM10FUVKPMSXGQO -->
