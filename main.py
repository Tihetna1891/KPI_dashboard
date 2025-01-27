from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import json
import logging
import asyncio
from datetime import date
import time
from db_pool import get_conn, release_conn

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateRange(BaseModel):
    start_date: date
    end_date: date

def load_config():
    with open("config.json", "r") as file:
        return json.load(file)
def initialize_db(conn, retries=3, delay=1):
    try:
        with conn.cursor() as cur:
            for attempt in range(retries):
                try:
                    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'dblink';")
                    result = cur.fetchall()
                    if not result:
                        cur.execute("CREATE EXTENSION dblink;")
                        logger.info("dblink extension is installed.")
                    break  # Exit the retry loop if successful
                except Exception as inner_e:
                    logger.warning(f"Attempt {attempt + 1} failed: {inner_e}")
                    time.sleep(delay)  # Wait before retrying
            else:
                logger.error(f"Failed to ensure dblink is installed after {retries} attempts.")
    except Exception as e:
        logger.error(f"Error ensuring dblink is installed: {e}")
@app.on_event("startup")
async def startup_event():
    conn = get_conn()
    if conn is not None:
        try:
            initialize_db(conn)  # Ensure dblink is installed during startup
        finally:
            release_conn(conn)
def df_to_json(df: pd.DataFrame) -> list:
    if df.empty:
        logger.warning("Empty DataFrame received.")
        return []

    # Clean column names
    df.columns = df.columns.str.strip()
    
    logger.info(f"DataFrame columns: {df.columns}")
    logger.info(f"DataFrame head:\n{df.head()}")

    if 'order_date' not in df.columns:
        logger.error("The 'order_date' column is missing from the DataFrame.")
        return []

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    return json.loads(df.to_json(orient='records'))
def fetch_query_sync(query: str, params: tuple) -> pd.DataFrame:
    conn = get_conn()
    
    if conn is None:
        logger.error("No available connection from the pool.")
        return pd.DataFrame()

    try:
        # initialize_db(conn)  # Ensure dblink is installed

        with conn.cursor() as cur:
            cur.execute(query, params)
            data = cur.fetchall()
            if not data:
                logger.warning(f"No data returned for query: {query}")
                return pd.DataFrame()

            columns_ = [desc[0] for desc in cur.description]
            if 'order_date' not in columns_:
                logger.error("The 'order_date' column is missing from the query result.")
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=columns_)

            logger.info(f"Query result head:\n{df.head()}")
            return df
    except Exception as e:
        logger.error(f"Error fetching data with query '{query}': {e}")
        return pd.DataFrame()
    finally:
        release_conn(conn)

@app.post("/fetch_aggregated_data/")
async def fetch_aggregated_data(date_range: DateRange):
    start_date = date_range.start_date
    end_date = date_range.end_date
    config = load_config()
    dbname = config["dbname"]
    
    queries = [
        
        f"""
        WITH total_accepted_orders_cte AS (
            SELECT 
                DATE(o.created_at) AS order_date,
                COUNT(DISTINCT o.id) AS total_accepted_orders -- Count each order only once
            FROM 
                orders o
            INNER JOIN 
                delivery_tracks dt ON (o.groups_carts_id = dt.group_cart_id OR o.personal_cart_id = dt.personal_cart_id)
            WHERE 
                o.status = 'COMPLETED'
                AND dt.created_at IS NOT NULL -- Make sure the order has at least one delivery track event
                
            GROUP BY 
                DATE(o.created_at)
        )
        SELECT 
            DATE(o.created_at) AS order_date,
            COUNT(*) AS total_orders,
            COUNT(CASE WHEN o.groups_carts_id IS NOT NULL THEN 1 END) AS group_order_count,
            COUNT(CASE WHEN o.groups_carts_id IS NOT NULL AND g.status = 'COMPLETED' THEN 1 END) AS completed_group_order_count,
            COUNT(CASE WHEN o.personal_cart_id IS NOT NULL THEN 1 END) AS personal_order_count,
            COALESCE(payments.name, o.payment_method) AS payment_method,
            COALESCE(tao.total_accepted_orders, 0) AS total_accepted_orders
        FROM 
            orders o
        LEFT JOIN 
            groups_carts gc ON o.groups_carts_id = gc.id
        LEFT JOIN 
            groups g ON gc.group_id = g.id
        LEFT JOIN 
            dblink(
                'dbname={dbname}',
                'SELECT pi2.third_party_payment_id AS order_id, t.status, pm.name
                FROM payment_intents pi2
                JOIN transactions t ON t.payment_intent_id = pi2.id
                JOIN payment_methods pm ON pm.id = t.payment_method_id'
            ) AS payments(order_id VARCHAR, status VARCHAR, name VARCHAR) 
            ON o.id::VARCHAR = payments.order_id AND payments.status = 'COMPLETED'
        LEFT JOIN 
            total_accepted_orders_cte tao ON DATE(o.created_at) = tao.order_date -- Bring in total accepted orders from the CTE
        WHERE 
            o.status = 'COMPLETED'
            AND DATE(o.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
        GROUP BY 
            DATE(o.created_at), COALESCE(payments.name, o.payment_method), tao.total_accepted_orders
        ORDER BY 
            order_date;

        """,
        f"""
        SELECT DATE(o.created_at) AS order_date, pn.name AS product_name,
               COALESCE(SUM(pci.quantity * p.weight), 0) AS personal_volume_sold,
               COALESCE(SUM(gc.quantity * p.weight), 0) AS group_volume_sold,
               COALESCE(SUM(pci.quantity * p.weight), 0) + COALESCE(SUM(gc.quantity * p.weight), 0) AS total_volume_sold
        FROM orders o
        LEFT JOIN personal_cart_items pci ON o.personal_cart_id = pci.cart_id
        LEFT JOIN groups_carts gc ON o.groups_carts_id = gc.id
        LEFT JOIN groups g ON gc.group_id = g.id
        LEFT JOIN group_deals gd ON g.group_deals_id = gd.id
        LEFT JOIN products p ON COALESCE(pci.product_id, gd.product_id) = p.id 
        JOIN product_names pn ON p.name_id = pn.id
        WHERE o.status = 'COMPLETED'
        AND DATE(o.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
        GROUP BY DATE(o.created_at), product_name;
        """,
        f"""
        SELECT DATE(o.created_at) AS order_date, pn.name AS product_name,
               COALESCE(SUM(pci.quantity * sd.original_price), 0) AS personal_revenue,
               COALESCE(SUM(gc.quantity * gd.group_price), 0) AS group_revenue,
               COALESCE(SUM(pci.quantity * sd.original_price), 0) + COALESCE(SUM(gc.quantity * gd.group_price), 0) AS total_revenue
        FROM orders o
        LEFT JOIN personal_cart_items pci ON o.personal_cart_id = pci.cart_id
        LEFT JOIN groups_carts gc ON o.groups_carts_id = gc.id
        LEFT JOIN groups g ON gc.group_id = g.id
        LEFT JOIN group_deals gd ON g.group_deals_id = gd.id
        LEFT JOIN products p ON COALESCE(pci.product_id, gd.product_id) = p.id
        LEFT JOIN single_deals sd ON p.id = sd.product_id
        JOIN product_names pn ON p.name_id = pn.id
        WHERE o.status = 'COMPLETED'
        AND DATE(o.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
        GROUP BY DATE(o.created_at), pn.name;
        """,
        f"""
        SELECT DATE(o.created_at) AS order_date, COUNT(*) AS total_received_orders,
               COUNT(CASE WHEN o.groups_carts_id IS NOT NULL THEN 1 END) AS group_order_recieved,
               COUNT(CASE WHEN o.personal_cart_id IS NOT NULL THEN 1 END) AS personal_order_recieved 
        FROM orders o
        WHERE o.status = 'COMPLETED'
        AND DATE(o.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
        GROUP BY DATE(o.created_at);
        """
    ]
    
   
    params = (start_date, end_date)

    try:
        results = await asyncio.to_thread(lambda: [fetch_query_sync(query, params) for query in queries])
    except Exception as e:
        logger.error(f"Error fetching queries: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from the database.")

    df_, df_total_volume_sold, df_total_revenue, df_received_orders = results

    try: 
        return {
            "aggregated_data": df_to_json(df_),
            "total_volume_sold_data": df_to_json(df_total_volume_sold),
            "received_orders_data": df_to_json(df_received_orders),
            "total_revenue_data": df_to_json(df_total_revenue),
        }
    except Exception as e:
        logger.error(f"Error processing DataFrames: {e}")
        raise HTTPException(status_code=500, detail="Error processing data.")