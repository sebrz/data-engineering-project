"""
Stock historical data processing DAG for Airflow.
This DAG processes CSV files containing historical stock data.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import psycopg2
import os
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 12),  # Updated to current date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG instance
dag = DAG(
    'historical_stock_data_processing',
    default_args=default_args,
    description='Process historical stock data from CSV files',
    schedule_interval='0 13 * * 1-5',  # 13:00 UTC / 9:00 AM ET on weekdays (before market open)
    catchup=False,
)

# Define the data directory
DATA_DIR = '/opt/airflow/data/processed'
PROCESSED_DIR = '/opt/airflow/data/processed'

def ensure_directories():
    """Ensure data directories exist"""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

def process_stock_prices():
    """Process historical stock price CSV files"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="financial_data",
        user="dataeng",
        password="dataeng"
    )
    cursor = conn.cursor()
    
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Look for CSV files in the data directory
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.csv') and 'prices' in filename:
            file_path = os.path.join(DATA_DIR, filename)
            logging.info(f"Processing file: {file_path}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Process each row
            for _, row in df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO historical_prices 
                    (date, symbol, open, high, low, close, volume, adj_close, batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, symbol) DO UPDATE 
                    SET 
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        adj_close = EXCLUDED.adj_close,
                        batch_id = EXCLUDED.batch_id,
                        processed_at = CURRENT_TIMESTAMP
                    """,
                    (
                        row['date'], row['symbol'], 
                        row['open'], row['high'], row['low'], row['close'],
                        row['volume'], row['adj_close'], batch_id
                    )
                )
            
            # Since we're processing files directly from the processed directory,
            # we'll just log that we've processed the file
            logging.info(f"Successfully processed file: {file_path}")
            
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info("Historical stock prices processing completed")

def process_fundamentals():
    """Process company fundamentals CSV files"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="financial_data",
        user="dataeng",
        password="dataeng"
    )
    cursor = conn.cursor()
    
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Look for CSV files in the data directory
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.csv') and 'fundamentals' in filename:
            file_path = os.path.join(DATA_DIR, filename)
            logging.info(f"Processing file: {file_path}")
            
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Process each row
            for _, row in df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO company_fundamentals 
                    (date, symbol, quarter, revenue_mil, eps, pe_ratio, target_price, analyst_rating, rating_count, batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, symbol, quarter) DO UPDATE 
                    SET 
                        revenue_mil = EXCLUDED.revenue_mil,
                        eps = EXCLUDED.eps,
                        pe_ratio = EXCLUDED.pe_ratio,
                        target_price = EXCLUDED.target_price,
                        analyst_rating = EXCLUDED.analyst_rating,
                        rating_count = EXCLUDED.rating_count,
                        batch_id = EXCLUDED.batch_id,
                        processed_at = CURRENT_TIMESTAMP
                    """,
                    (
                        row['date'], row['symbol'], row['quarter'],
                        row['revenue_mil'], row['eps'], row['pe_ratio'],
                        row['target_price'], row['analyst_rating'], row['rating_count'], 
                        batch_id
                    )
                )
            
            # Since we're processing files directly from the processed directory,
            # we'll just log that we've processed the file
            logging.info(f"Successfully processed file: {file_path}")
            
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info("Company fundamentals processing completed")

def create_daily_summary():
    """Create daily summaries from historical data"""
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="financial_data",
        user="dataeng",
        password="dataeng"
    )
    cursor = conn.cursor()
    
    # Truncate and regenerate daily summary from historical data
    cursor.execute(
        """
        INSERT INTO daily_summary
        (date, symbol, open, high, low, close, avg_price, total_volume, price_change_pct, num_trades, source)
        SELECT 
            h.date,
            h.symbol,
            h.open,
            h.high,
            h.low,
            h.close,
            (h.high + h.low + h.close) / 3 as avg_price,
            h.volume as total_volume,
            ((h.close - h.open) / h.open) * 100 as price_change_pct,
            1 as num_trades,
            'batch' as source
        FROM 
            historical_prices h
        WHERE
            h.date >= CURRENT_DATE - INTERVAL '30 days'
        ON CONFLICT (date, symbol, source) 
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            avg_price = EXCLUDED.avg_price,
            total_volume = EXCLUDED.total_volume,
            price_change_pct = EXCLUDED.price_change_pct,
            created_at = CURRENT_TIMESTAMP
        """
    )
            
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info("Daily summary creation completed")

# Task to ensure directories
ensure_directories_task = PythonOperator(
    task_id='ensure_directories',
    python_callable=ensure_directories,
    dag=dag,
)

# Task to process stock prices
process_stock_prices_task = PythonOperator(
    task_id='process_stock_prices',
    python_callable=process_stock_prices,
    dag=dag,
)

# Task to process fundamentals
process_fundamentals_task = PythonOperator(
    task_id='process_fundamentals',
    python_callable=process_fundamentals,
    dag=dag,
)

# Task to create daily summary
create_daily_summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag,
)

# Define task dependencies
ensure_directories_task >> [process_stock_prices_task, process_fundamentals_task] >> create_daily_summary_task