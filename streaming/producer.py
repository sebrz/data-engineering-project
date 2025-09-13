"""
Stock tick producer simulation for the financial data pipeline.
Generates synthetic stock price data and sends it to Kafka.
"""
import json
import time
import random
import logging
from datetime import datetime
import uuid
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sample stock data
STOCKS = {
    'AAPL': {'price': 180.0, 'volatility': 0.02},
    'MSFT': {'price': 320.0, 'volatility': 0.018},
    'GOOGL': {'price': 138.0, 'volatility': 0.025}
}

EXCHANGES = ['NASDAQ', 'NYSE', 'IEX']

def generate_stock_tick(symbol):
    """Generate a realistic stock tick with price movement"""
    stock = STOCKS[symbol]
    
    # Simulate price movement with random walk
    price_change_pct = random.normalvariate(0, stock['volatility'])
    new_price = stock['price'] * (1 + price_change_pct)
    
    # Update the base price for next time
    STOCKS[symbol]['price'] = new_price
    
    # Random volume between 100-10000 shares, more likely to be lower
    volume = int(random.paretovariate(1.5) * 100) % 10000 
    if volume < 100:
        volume = 100
        
    # Generate bid/ask spread
    spread = new_price * 0.0005  # 0.05% spread
    bid = new_price - spread/2
    ask = new_price + spread/2
    
    tick = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'symbol': symbol,
        'price': round(new_price, 2),
        'volume': volume,
        'bid': round(bid, 2),
        'ask': round(ask, 2),
        'exchange': random.choice(EXCHANGES)
    }
    
    return tick

def generate_order(symbol):
    """Generate a simulated trading order"""
    stock = STOCKS[symbol]
    
    # Randomize order properties
    side = random.choice(['BUY', 'SELL'])
    order_type = random.choice(['MARKET', 'LIMIT'])
    quantity = random.randint(1, 100) * 10  # Round to multiples of 10
    
    # Price logic
    if order_type == 'MARKET':
        price = stock['price']
    else:  # LIMIT order
        if side == 'BUY':
            # Buy limit usually below market
            price = stock['price'] * (1 - random.uniform(0.001, 0.01))
        else:
            # Sell limit usually above market
            price = stock['price'] * (1 + random.uniform(0.001, 0.01))
    
    order = {
        'order_id': f"o{uuid.uuid4().hex[:10]}",
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'symbol': symbol,
        'order_type': order_type,
        'side': side,
        'quantity': quantity,
        'price': round(price, 2),
        'trader_id': f"t{random.randint(1000, 9999)}",
        'status': 'EXECUTED'
    }
    
    return order

def connect_to_kafka():
    """Establish connection to Kafka with retry logic"""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise

def main():
    """Main function to produce stock data messages"""
    logger.info("Starting stock data producer")
    
    try:
        producer = connect_to_kafka()
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        return
    
    try:
        while True:
            # Generate and send stock ticks
            for symbol in STOCKS.keys():
                # Generate tick data
                tick_data = generate_stock_tick(symbol)
                producer.send('stock-ticks', tick_data)
                logger.info(f"Sent tick: {symbol} at ${tick_data['price']:.2f}")
                
                # Occasionally generate orders (20% chance per tick)
                if random.random() < 0.2:
                    order_data = generate_order(symbol)
                    producer.send('trading-orders', order_data)
                    logger.info(f"Sent order: {order_data['side']} {order_data['quantity']} {symbol}")
            
            # Ensure messages are sent
            producer.flush()
            
            # Sleep for a random time between 1-3 seconds
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
        logger.info("Producer shutdown")

if __name__ == "__main__":
    # Wait for Kafka to be ready
    time.sleep(20)
    main()