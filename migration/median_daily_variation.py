import sqlite3
import csv


def sqlite_query(datafile):
    conn = sqlite3.connect(datafile)
    cursor = conn.cursor()

    # Execute the query
    cursor.execute("""
        WITH daily_variations AS (
        SELECT 
            company,
            (high - low) AS daily_variation
        FROM 
            stock_data
        ),
        ranked_variations AS (
            SELECT 
                company,
                daily_variation,
                ROW_NUMBER() OVER (PARTITION BY company ORDER BY daily_variation) AS row_asc,
                ROW_NUMBER() OVER (PARTITION BY company ORDER BY daily_variation DESC) AS row_desc
            FROM 
                daily_variations
        )
        SELECT 
            company,
            AVG(daily_variation) AS median_daily_variation
        FROM 
            ranked_variations
        WHERE 
            row_asc IN (row_desc, row_desc - 1, row_desc + 1)
        GROUP BY 
            company
        """)

    # Fetch all rows from the result set
    rows = cursor.fetchall()
    return rows

def write_results_to_csv(results,output_csv_file):
    """Writes the results to a CSV file."""
    with open(output_csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Company','median_daily_variation'])
        csv_writer.writerows(results)
    return output_csv_file

def main():
    # Example usage:
    try :
        csv_file = "median_daily_variation.csv"
        db_file = 'stock_data.db'  # Path to the output SQLite database file
        rows = sqlite_query(db_file)
        write_results_to_csv(rows,output_csv_file = csv_file)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()