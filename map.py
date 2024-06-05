import streamlit as st
import pandas as pd
import psycopg2 

@st.cache_data
def get_orders(start_date, end_date):
    conn_str = st.secrets["url"]

    # Establish connection
    conn = psycopg2.connect(conn_str)

    # Create a cursor
    cur = conn.cursor()

    # Define the query
    query = """
    SELECT
        o.id AS order_id,
        o.groups_carts_id,
        o.total_amount,
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
        o.created_at BETWEEN %(start_date)s AND %(end_date)s;
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
@st.cache_data
def get_product():
    conn_str = st.secrets["url"]

    # Establish connection
    conn = psycopg2.connect(conn_str)

    # Create a cursor
    cur = conn.cursor()

    # Execute a query
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
    # Close the cursor and connection
    cur.close()
    conn.close()

    df=pd.DataFrame(results, columns=col)
    return df
@st.cache_data
def get_product_names():
    conn_str = st.secrets["url"]

    # Establish connection
    conn = psycopg2.connect(conn_str)

    # Create a cursor
    cur = conn.cursor()

    # Execute a query
    cur.execute("""
    SELECT 
        pn.id AS name_id,
        pn.name AS product_name
    FROM 
        product_names pn;
    """)
    # Fetch results
    results = cur.fetchall()
    col = [desc[0] for desc in cur.description]
    # Close the cursor and connection
    cur.close()
    conn.close()

    df=pd.DataFrame(results, columns=col)
    return df
@st.cache_data
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
@st.cache_data
def calculate_total_sales(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name', 'product_name','date', 'total_sales']
    return total_sales
@st.cache_data
def calculate_total_sales_vendors(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name',  'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name','date', 'total_sales']
    return total_sales
@st.cache_data
def calculate_order_volume(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','product_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name','product_name', 'date', 'order_count']
    return order_volume
@st.cache_data
def calculate_order_volume_vendor(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name', 'date', 'order_count']
    return order_volume
@st.cache_data
# Example function to calculate order volume by status
def calculate_order_volume_by_status(orders,products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    if 'vendor_name' not in merged_data.columns:
        merged_data['vendor_name'] = merged_data['vendor_id'].map(products.set_index('vendor_id')['vendor_name'])
    # Group by vendor_name, product_name, and status to calculate order volume
    order_volume_by_status = merged_data.groupby(['vendor_name', 'product_name', 'time_frame','status']).size().reset_index(name='order_volume')

    # order_volume_by_status = orders.groupby(['vendor_name','product_name','time_frame','status']).size().reset_index(name='order_volume')
    order_volume_by_status.columns = ['vendor_name','product_name', 'date','status', 'order_volume']
    return order_volume_by_status
@st.cache_data
def calculate_average_order_value(orders, products, time_frame):
    # products = products.merge(product_names, on='name_id')
    merged_data = orders.merge(products, on='product_id')
    # merged_data['order_value'] = merged_data['total_amount'/] 
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','product_name', 'date', 'average_order_value']
    return average_order_value
@st.cache_data
def calculate_average_order_value_vendor(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','date', 'average_order_value']
    return average_order_value
@st.cache_data
def calculate_fulfillment_time(orders, products, time_frame):
    orders = orders.merge(products, on='product_id')
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['updated_at'] = pd.to_datetime(orders['updated_at'])
    orders['fulfillment_time'] = (orders['updated_at'] - orders['created_at']).dt.total_seconds() / 3600  # in hours
    orders = aggregate_data(orders, time_frame, date_column='created_at')
    fulfillment_time = orders.groupby(['vendor_name','product_name',  'time_frame'])['fulfillment_time'].mean().reset_index()
    return fulfillment_time[['vendor_name', 'product_name','time_frame', 'fulfillment_time']]
@st.cache_data
def calculate_fulfillment_time_vendor(orders, products, time_frame):
    # products = products.merge(product_names, on='name_id')
    orders = orders.merge(products, on='product_id')
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['updated_at'] = pd.to_datetime(orders['updated_at'])
    orders['fulfillment_time'] = (orders['updated_at'] - orders['created_at']).dt.total_seconds() / 3600  # in hours
    orders = aggregate_data(orders, time_frame, date_column='created_at')
    fulfillment_time = orders.groupby(['vendor_name', 'time_frame'])['fulfillment_time'].mean().reset_index()
    return fulfillment_time[['vendor_name', 'time_frame', 'fulfillment_time']]
@st.cache_data
def product_popularity(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    product_popularity = merged_data.groupby(['vendor_name', 'product_name','time_frame'])['quantity'].sum().reset_index()
    product_popularity.columns = ['vendor_name','product_name', 'time_frame',  'product_popularity']
    return product_popularity
@st.cache_data
def product_popularity_vendor(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    product_popularity = merged_data.groupby(['vendor_name','time_frame'])['quantity'].sum().reset_index()
    product_popularity.columns = ['vendor_name','time_frame',  'product_popularity']
    return product_popularity
