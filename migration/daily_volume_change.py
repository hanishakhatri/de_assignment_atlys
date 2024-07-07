import sqlite3
import csv


def sqlite_query(datafile):
    conn = sqlite3.connect(datafile)
    cursor = conn.cursor()

    # Execute the query
    cursor.execute("""
        WITH volume_changes AS (
        SELECT
            Company,
            Date,
            Volume,
            LAG(Volume) OVER (PARTITION BY Company ORDER BY Date) AS previous_volume,
            Volume - LAG(Volume) OVER (PARTITION BY Company ORDER BY Date) AS volume_change
        FROM
            stock_data
        )
        SELECT
            Company,
            Date,
            COALESCE(volume_change, 0) AS Volume_Change
        FROM
            volume_changes
        ORDER BY
            Company,
            Date;

        """)

    # Fetch all rows from the result set
    rows = cursor.fetchall()
    return rows

def write_results_to_csv(results,output_csv_file):
    """Writes the results to a CSV file."""
    with open(output_csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Company','Date','Volume_Change'])
        csv_writer.writerows(results)
    return output_csv_file

def main():
    # Example usage:
    try :
        csv_file = "daily_volume_change.csv"
        db_file = 'stock_data.db'  # Path to the output SQLite database file
        rows = sqlite_query(db_file)
        write_results_to_csv(rows,output_csv_file = csv_file)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()