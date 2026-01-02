import requests
import logging
from config import API_TOKEN, API_BASE_URL
import time
from requests.exceptions import RequestException, Timeout, HTTPError
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_data(dataset_type: str = 'ecommerce', rows: int = 1000) -> dict:
    """Obtain data from the API."""
    url = f"{API_BASE_URL}/datasets.php"
    params = {
        'type': dataset_type,
        'rows': rows,
        'token': API_TOKEN
    }
    
    logger.info(f"Fetching {rows} rows of {dataset_type} data...")
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()  # Exception if HTTP error
    
    data = response.json()
    logger.info(f"Received {len(data.get('tables', {}).get('orders', []))} orders")
    
    return data

def fetch_data_with_retry(
    dataset_type: str = 'ecommerce', 
    rows: int = 1000,
    max_retries: int = 3,
    backoff_factor: float = 2.0
) -> dict:
    """Obtain data with automatic retries."""
    
    for attempt in range(max_retries):
        try:
            return fetch_data(dataset_type, rows)
            
        except Timeout:
            logger.warning(f"Timeout in attempt {attempt + 1}/{max_retries}")
            
        except HTTPError as e:
            if e.response.status_code >= 500:
                logger.warning(f"Server error: {e}")
            else:
                # Errors 4xx not retried
                logger.error(f"Client error: {e}")
                raise
                
        except RequestException as e:
            logger.warning(f"Connection error: {e}")
        
        if attempt < max_retries - 1:
            wait_time = backoff_factor ** attempt
            logger.info(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    raise Exception(f"Failed after {max_retries} attempts")

def transform_data(raw_data: dict) -> pd.DataFrame:
    """Transform and enrich the data."""
    logger.info("Transforming data...")
    
    # Extract orders table
    orders = raw_data.get('tables', {}).get('orders', [])
    df = pd.DataFrame(orders)
    
    if df.empty:
        logger.warning("No data to process")
        return df
    
    # Convert types
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    
    # Add calculated fields
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    df['order_year'] = df['order_date'].dt.year
    df['is_high_value'] = df['total_amount'] > 100
    df['day_of_week'] = df['order_date'].dt.day_name()
    
    # Validations
    invalid_totals = df['total_amount'].isna().sum()
    if invalid_totals > 0:
        logger.warning(f"{invalid_totals} orders with invalid total")
    
    logger.info(f"Transformed {len(df)} orders")
    return df

def save_data(df: pd.DataFrame, output_dir: str = 'output'):
    """Save data partitioned by month."""
    logger.info(f"Saving data to {output_dir}/...")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save partitioned by month
    df.to_parquet(
        f'{output_dir}/orders',
        partition_cols=['order_year', 'order_month'],
        index=False
    )
    
    # Save consolidated file
    df.to_parquet(f'{output_dir}/orders_all.parquet', index=False)
    
    # Statistics
    logger.info(f"Saved {len(df)} orders")
    logger.info(f"Partitions: {df['order_month'].nunique()} months")

def main():
    """Pipeline main."""
    logger.info("=" * 50)
    logger.info("API Pipeline - Starting")
    logger.info("=" * 50)
    
    try:
        # Extract
        raw_data = fetch_data_with_retry(rows=5000)
        
        # Transform
        df = transform_data(raw_data)
        
        if df.empty:
            logger.error("No data to save")
            return
        
        # Load
        save_data(df)
        
        logger.info("=" * 50)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()