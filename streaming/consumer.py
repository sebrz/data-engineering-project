"""
Real-time stock tick consumer for the financial data pipeline.
Consumes messages from Kafka and processes them before storing in PostgreSQL.
"""
import json
import time
import logging
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Establish connection to PostgreSQL database with retry logic"""
    max_retries = 20
    retry_delay = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="financial_data",
                user="dataeng",
                password="dataeng"
            )
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection attempt {attempt+1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to PostgreSQL after {max_retries} attempts")
                raise

def connect_to_kafka():
    """Establish connection to Kafka with retry logic"""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'stock-ticks',
                bootstrap_servers=['kafka:29092'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='stock-processor',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise

def calculate_price_change(conn, symbol, current_price):
    """Calculate percentage price change from the previous price"""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT price FROM realtime_ticks 
            WHERE symbol = %s 
            ORDER BY timestamp DESC 
            LIMIT 1
            """, 
            (symbol,)
        )
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            previous_price = result[0]
            if previous_price > 0:
                # Convert Decimal to float for consistent calculation
                previous_price_float = float(previous_price)
                return ((current_price - previous_price_float) / previous_price_float) * 100
        
        return 0.0  # Default if no previous price
    except Exception as e:
        logger.error(f"Error calculating price change: {e}")
        return 0.0

def check_day_high_low(conn, symbol, current_price):
    """Check if current price is day high or day low"""
    today = datetime.now().date()
    try:
        cursor = conn.cursor()
        
        # Get day high
        cursor.execute(
            """
            SELECT MAX(price) FROM realtime_ticks 
            WHERE symbol = %s AND DATE(timestamp) = %s
            """, 
            (symbol, today)
        )
        day_high_result = cursor.fetchone()
        
        # Get day low
        cursor.execute(
            """
            SELECT MIN(price) FROM realtime_ticks 
            WHERE symbol = %s AND DATE(timestamp) = %s
            """, 
            (symbol, today)
        )
        day_low_result = cursor.fetchone()
        cursor.close()
        
        day_high = False
        day_low = False
        
        if day_high_result and day_high_result[0]:
            day_high = current_price >= day_high_result[0]
            
        if day_low_result and day_low_result[0]:
            day_low = current_price <= day_low_result[0]
            
        return day_high, day_low
    
    except Exception as e:
        logger.error(f"Error checking day high/low: {e}")
        return False, False

def process_tick(conn, tick_data):
    """Process a stock tick and store in database with derived fields"""
    try:
        cursor = conn.cursor()
        
        # Extract base data
        timestamp = datetime.fromisoformat(tick_data['timestamp'].replace('Z', '+00:00'))
        symbol = tick_data['symbol']
        price = tick_data['price']
        volume = tick_data['volume']
        bid = tick_data.get('bid')
        ask = tick_data.get('ask')
        exchange = tick_data.get('exchange')
        
        # Calculate derived fields
        price_change_pct = calculate_price_change(conn, symbol, price)
        day_high, day_low = check_day_high_low(conn, symbol, price)
        
        # Insert into database
        cursor.execute(
            """
            INSERT INTO realtime_ticks 
            (timestamp, symbol, price, volume, bid, ask, exchange, price_change_pct, day_high, day_low)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (timestamp, symbol, price, volume, bid, ask, exchange, price_change_pct, day_high, day_low)
        )
        conn.commit()
        cursor.close()
        
        logger.info(f"Processed tick for {symbol} at {price} ({price_change_pct:.2f}%)")
        
    except Exception as e:
        logger.error(f"Error processing tick: {e}")
        conn.rollback()

def main():
    """Main function to consume and process messages"""
    logger.info("Starting stock tick consumer")
    
    # Connect to services
    try:
        conn = connect_to_db()
        consumer = connect_to_kafka()
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        return
    
    try:
        logger.info("Waiting for messages...")
        for message in consumer:
            tick_data = message.value
            logger.info(f"Received tick: {tick_data}")
            process_tick(conn, tick_data)
            
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.close()
        logger.info("Consumer shutdown")

if __name__ == "__main__":
    # Wait for Kafka and Postgres to be ready
    logger.info("Waiting for services to be ready...")
    time.sleep(30)
    main()