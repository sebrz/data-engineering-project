"""
Data generator utility for creating sample data.
This script generates:
1. Historical stock prices (CSV files)
2. Company fundamentals (CSV files)
3. Real-time stock ticks (for Kafka streaming)
"""
import pandas as pd
import numpy as np
import os
import random
import time
import json
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Create data directory if it doesn't exist
DEFAULT_DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "processed")
os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)

# Sample stock data
STOCKS = {
    'AAPL': {'start_price': 150.0, 'volatility': 0.015, 'trend': 0.0005},
    'MSFT': {'start_price': 290.0, 'volatility': 0.012, 'trend': 0.0008},
    'GOOGL': {'start_price': 120.0, 'volatility': 0.018, 'trend': 0.0003}
}

# Sample fundamentals data
FUNDAMENTALS = {
    'AAPL': {'revenue_base': 90, 'eps_base': 1.4, 'pe_base': 25, 'target_growth': 0.02},
    'MSFT': {'revenue_base': 50, 'eps_base': 2.3, 'pe_base': 30, 'target_growth': 0.015},
    'GOOGL': {'revenue_base': 65, 'eps_base': 1.2, 'pe_base': 22, 'target_growth': 0.01}
}

def generate_historical_prices(start_date, end_date, output_dir=DEFAULT_DATA_DIR):
    """Generate historical stock price data"""
    os.makedirs(output_dir, exist_ok=True)
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
    data = []
    for symbol, stock_info in STOCKS.items():
        price = stock_info['start_price']
        for date in date_range:
            daily_return = np.random.normal(stock_info['trend'], stock_info['volatility'])
            price *= (1 + daily_return)
            daily_volatility = stock_info['volatility'] * price
            open_price = price * (1 + np.random.normal(0, 0.2 * stock_info['volatility']))
            high_price = max(open_price, price) * (1 + abs(np.random.normal(0, 0.5 * stock_info['volatility'])))
            low_price = min(open_price, price) * (1 - abs(np.random.normal(0, 0.5 * stock_info['volatility'])))
            close_price = price
            volume_base = abs(daily_return) * 10000000
            volume = int(np.random.normal(volume_base, volume_base * 0.3))
            volume = max(volume, 100000)
            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'symbol': symbol,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'adj_close': round(close_price, 2)
            })
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    file_path = os.path.join(output_dir, f"historical_prices_{datetime.now().strftime('%Y%m%d')}.csv")
    df.to_csv(file_path, index=False)
    print(f"Generated historical prices: {file_path}")
    return file_path

def generate_company_fundamentals(date_range, output_dir=DEFAULT_DATA_DIR):
    """Generate company fundamentals data (quarterly)"""
    os.makedirs(output_dir, exist_ok=True)
    quarters = []
    for year in range(date_range[0].year, date_range[-1].year + 1):
        for quarter in range(1, 5):
            quarter_date = datetime(year, (quarter-1)*3+1, 15)
            if date_range[0] <= quarter_date <= date_range[-1]:
                quarters.append((year, quarter, quarter_date))
    data = []
    for symbol, fund_info in FUNDAMENTALS.items():
        for year, quarter, quarter_date in quarters:
            quarters_passed = (year - date_range[0].year) * 4 + quarter
            revenue_growth = 1 + (fund_info['target_growth'] * quarters_passed) + np.random.normal(0, 0.02)
            revenue = fund_info['revenue_base'] * revenue_growth
            eps_growth = revenue_growth * (1 + np.random.normal(0, 0.1))
            eps = fund_info['eps_base'] * eps_growth
            pe_ratio = fund_info['pe_base'] * (1 + np.random.normal(0, 0.08))
            forward_pe = pe_ratio * (1 + np.random.normal(0, 0.05))
            target_price = eps * forward_pe
            rating_options = ['BUY', 'BUY', 'HOLD', 'SELL']
            if eps_growth > 1.05:
                rating_weights = [0.6, 0.25, 0.1, 0.05]
            else:
                rating_weights = [0.3, 0.3, 0.3, 0.1]
            analyst_rating = random.choices(rating_options, weights=rating_weights)[0]
            rating_count = random.randint(8, 25)
            data.append({
                'date': quarter_date.strftime('%Y-%m-%d'),
                'symbol': symbol,
                'quarter': f"Q{quarter}",
                'revenue_mil': round(revenue, 2),
                'eps': round(eps, 2),
                'pe_ratio': round(pe_ratio, 2),
                'target_price': round(target_price, 2),
                'analyst_rating': analyst_rating,
                'rating_count': rating_count
            })
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    file_path = os.path.join(output_dir, f"company_fundamentals_{datetime.now().strftime('%Y%m%d')}.csv")
    df.to_csv(file_path, index=False)
    print(f"Generated company fundamentals: {file_path}")
    return file_path

def generate_realtime_ticks(producer=None, duration=60, tick_interval=0.5):
    """
    Generate real-time stock price ticks for the streaming component
    
    Args:
        producer: KafkaProducer instance. If None, ticks are just printed to console.
        duration: Duration in seconds to generate ticks for
        tick_interval: Interval between ticks in seconds
    """
    print(f"Generating real-time ticks for {duration} seconds...")
    
    # Latest stock prices
    current_prices = {}
    for symbol, stock_info in STOCKS.items():
        current_prices[symbol] = stock_info['start_price']
    
    # Track data for each stock
    tick_data = {symbol: {'price': price, 'volume': 0, 'trades': 0} 
                for symbol, price in current_prices.items()}
    
    # Generate ticks
    end_time = time.time() + duration
    while time.time() < end_time:
        # Pick a random stock
        symbol = random.choice(list(STOCKS.keys()))
        stock_info = STOCKS[symbol]
        
        # Update price with noise
        price_change = np.random.normal(stock_info['trend'], stock_info['volatility'])
        tick_data[symbol]['price'] *= (1 + price_change)
        
        # Generate trade data
        trade_size = int(np.random.exponential(500)) * 10  # Realistic trade sizes
        tick_data[symbol]['volume'] += trade_size
        tick_data[symbol]['trades'] += 1
        
        # Create tick data
        current_price = tick_data[symbol]['price']
        spread = current_price * random.uniform(0.0005, 0.002)  # 0.05% to 0.2% spread
        
        tick = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'price': round(current_price, 2),
            'volume': trade_size,
            'bid': round(current_price - spread/2, 2),  # Bid slightly below price
            'ask': round(current_price + spread/2, 2),  # Ask slightly above price
            'trade_id': f"T{int(time.time()*1000) % 100000000}",
            'exchange': random.choice(['NYSE', 'NASDAQ', 'IEX']),
            'trade_type': random.choice(['market', 'limit', 'stop']),
        }
        
        # Send to Kafka or print
        if producer:
            try:
                producer.send('stock-ticks', json.dumps(tick).encode('utf-8'))
                print(f"Sent tick to Kafka: {tick['symbol']} @ ${tick['price']}")
            except Exception as e:
                print(f"Error sending to Kafka: {e}")
        else:
            print(f"Tick: {tick}")
        
        # Sleep between ticks
        time.sleep(tick_interval)
    
    print(f"Generated {sum(d['trades'] for d in tick_data.values())} ticks over {duration} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate financial data for testing')
    parser.add_argument('--mode', choices=['historical', 'streaming', 'all'], default='all',
                        help='Type of data to generate (historical, streaming, or all)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration in seconds to generate streaming data')
    parser.add_argument('--output_dir', type=str, default=DEFAULT_DATA_DIR,
                        help='Directory to save generated CSV files')
    args = parser.parse_args()

    # Generate historical data
    if args.mode in ['historical', 'all']:
        print("Generating historical data...")
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=730)  # ~2 years
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')
        generate_historical_prices(start_date, end_date, output_dir=args.output_dir)
        generate_company_fundamentals(date_range, output_dir=args.output_dir)

    # Generate streaming data
    if args.mode in ['streaming', 'all']:
        try:
            print("Connecting to Kafka...")
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            generate_realtime_ticks(producer, duration=args.duration)
        except Exception as e:
            print(f"Error with Kafka, falling back to console output: {e}")
            generate_realtime_ticks(None, duration=args.duration)
        finally:
            if 'producer' in locals() and producer is not None:
                producer.flush()
                producer.close()