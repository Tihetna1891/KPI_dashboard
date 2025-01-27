import streamlit as st
import pandas as pd
from db_pool import get_conn, release_conn

@st.cache_data
def get_orders(start_date, end_date):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Define the query
            query = """
            SELECT
                o.id AS order_id,
                o.groups_carts_id,
                o.total_amount,
                o.discount,
                o.created_at,
                o.status,
                o.response,
                o.updated_at,
                gd.product_id,
                gd.group_price,
                gc.quantity
            FROM
                orders o
            JOIN
                groups_carts gc ON o.groups_carts_id = gc.id
            JOIN
                groups g ON gc.group_id = g.id
            JOIN
                group_deals gd ON g.group_deals_id = gd.id
            WHERE
                o.created_at BETWEEN %(start_date)s AND %(end_date)s + interval '1 day' - interval '1 second'
                
            """

            # Execute the query with parameters
            cur.execute(query, {"start_date": start_date, "end_date": end_date})

            # Fetch results
            results = cur.fetchall()
            col = [desc[0] for desc in cur.description]

            # Close the cursor and connection
            cur.close()
            conn.close()

            # Create a DataFrame from the results
            df = pd.DataFrame(results, columns=col)
            return df
    finally:
        if conn:
            release_conn(conn)
@st.cache_data
def get_products():
    conn = get_conn()
    # Execute a query
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT 
                p.id AS product_id,
                p.vendor_id,
                p.name_id,
                p.stock_alert,
                v.name AS vendor_name
            FROM 
                products p
            JOIN 
                vendors v ON p.vendor_id = v.id;
            """)
            # Fetch results
            results = cur.fetchall()
            col = [desc[0] for desc in cur.description]

            df=pd.DataFrame(results, columns=col)
            return df
    finally:
            if conn:
                release_conn(conn)
@st.cache_data
def get_product_names():
    conn = get_conn()
    # Execute a query
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT 
                pn.id AS name_id,
                pn.category_id,
                pn.name AS product_name
            FROM 
                product_names pn;
            """)
            # Fetch results
            results = cur.fetchall()
            col = [desc[0] for desc in cur.description]  

            df=pd.DataFrame(results, columns=col)
            return df
    finally:
        if conn:
            release_conn(conn)

def get_categories():
    conn = get_conn()

    # Execute a query
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT 
            c.id as category_id,
            c.name as category_name,
            c.short_description,
            c.long_description

            FROM 
            categories c;
            """)
            # Fetch results
            results = cur.fetchall()
            col = [desc[0] for desc in cur.description]
        
            # Fetch categories data from the database
            df=pd.DataFrame(results, columns=col)
            return df
    finally:
        if conn:
            release_conn(conn)
    
@st.cache_data
def get_vendors():
  
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Execute a query
            cur.execute("""
            SELECT 
                vn.id AS vendor_id,
                vn.created_at,
                vn.name AS vendor_name
            FROM 
                vendors vn;
            """)
            # Fetch results
            results = cur.fetchall()
            col = [desc[0] for desc in cur.description]
            # Close the cursor and connection
            cur.close()
            conn.close()

            df=pd.DataFrame(results, columns=col)
            return df
    finally:
        if conn:
            release_conn(conn)

def aggregate_data(df, time_frame, date_column='created_at'):
    df[date_column] = pd.to_datetime(df[date_column])
    if time_frame == "Daily":
        df['time_frame'] = df[date_column].dt.date
    elif time_frame == "Weekly":
        df['time_frame'] = df[date_column].dt.to_period('W').apply(lambda r: r.start_time)
    elif time_frame == "Monthly":
        df['time_frame'] = df[date_column].dt.to_period('M').apply(lambda r: r.start_time)
    elif time_frame == "Yearly":
        df['time_frame'] = df[date_column].dt.to_period('Y').apply(lambda r: r.start_time)
    return df

def calculate_category_sales(orders, products, time_frame):
    # Filter completed orders
    completed_orders = orders[orders['status'] == 'COMPLETED']
    
    # Merge completed orders with products on product_id
    merged_data = completed_orders.merge(products, on='product_id')
    
    # Calculate total sales for each order
    merged_data['total_sales'] = merged_data['total_amount'] - merged_data['discount']
    
    # Aggregate the data by time frame
    merged_data = aggregate_data(merged_data, time_frame)
    
    # Calculate category sales
    category_sales = merged_data.groupby(['category_name', 'time_frame'])['total_sales'].sum().reset_index()
    category_sales.columns = ['category_name', 'time_frame', 'category_sales']
    
    # Calculate number of completed orders per category
    orders_per_category = merged_data.groupby(['category_name']).size().reset_index(name='num_completed_orders')
    
    # Calculate number of unique products per category
    products_per_category = products.groupby('category_name').size().reset_index(name='num_products')
    
    # Merge the calculated data
    result = category_sales.merge(orders_per_category, on=['category_name'], how='left')
    result = result.merge(products_per_category, on='category_name', how='left')
    
    # Create a new column for category names with numbers in brackets
    result['category_name_with_numbers'] = result.apply(lambda row: f"{row['category_name']} ({row['num_completed_orders']} orders, {row['num_products']} products)", axis=1)
    
    return result
def calculate_category_sales_vendors(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data['total_sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    category_sales = merged_data.groupby(['vendor_name','category_name', 'time_frame'])['total_sales'].sum().reset_index()
    category_sales.columns = ['vendor_name','category_name', 'time_frame', 'category_sales']
    return category_sales
def calculate_total_sales(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name', 'product_name','time_frame', 'total_sales']
    return total_sales


def calculate_total_sales_vendors(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name',  'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name','time_frame', 'total_sales']
    return total_sales

def calculate_order_volume(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','product_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name','product_name', 'time_frame', 'order_count']
    return order_volume

def calculate_order_volume_vendor(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name', 'time_frame', 'order_count']
    return order_volume
def calculate_average_order_value(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','product_name', 'time_frame', 'average_order_value']
    return average_order_value

def calculate_average_order_value_vendor(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','time_frame', 'average_order_value']
    return average_order_value

def product_sales(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    product_sales = merged_data.groupby(['vendor_name', 'product_name','time_frame'])['sales'].sum().reset_index()
    product_sales.columns = ['vendor_name','product_name', 'time_frame',  'product_sales']
    return product_sales

def product_sales_vendor(orders, products, time_frame):
    # merged_data = orders.merge(products, on='product_id')
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    product_sales = merged_data.groupby(['vendor_name','time_frame'])['sales'].sum().reset_index()
    product_sales.columns = ['vendor_name','time_frame',  'product_sales']
    return product_sales

def calculate_product_portfolio(orders, products, time_frame):
    # Filter completed orders
    completed_orders = orders[orders['status'] == 'COMPLETED']
    
    # Merge completed orders with products on product_id
    merged_data = completed_orders.merge(products, on='product_id')
    
    merged_data = aggregate_data(merged_data, time_frame)
        
    # Calculate the number of sold products per vendor
    sold_products_per_vendor = merged_data.groupby(['vendor_name','time_frame'])['product_id'].nunique().reset_index()
    sold_products_per_vendor.columns = ['vendor_name','time_frame', 'sold_product_count']
    
    # Calculate the total number of products per vendor
    total_products_per_vendor = products.groupby(['vendor_name'])['product_id'].nunique().reset_index()
    total_products_per_vendor.columns = ['vendor_name', 'total_product_count']
    
    # Merge the two dataframes
    product_portfolio = sold_products_per_vendor.merge(total_products_per_vendor, on='vendor_name')
    
    product_portfolio['vendor_name_with_total'] = product_portfolio.apply(
        lambda row: f"{row['vendor_name']} ({row['total_product_count']} products)", axis=1)
    
    return product_portfolio


