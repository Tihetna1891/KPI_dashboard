import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import folium
from streamlit_folium import folium_static
# from folium.plugins import HeatMap, Fullscreen
import numpy as np
import altair as alt
import re
from datetime import datetime, timedelta
import io
import sys
sys.path.append('../')
from scripts.Loader import load_data
import altair as alt
import os
# Streamlit date input and time frame selection
# start_date = st.sidebar.date_input("Start date", datetime(2023, 1, 1))
# end_date = st.sidebar.date_input("End date", datetime.today())
# time_frame = st.sidebar.selectbox("Select time frame", ["Daily", "Weekly", "Monthly", "Yearly"])


import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Database connection details
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# SQLAlchemy connection string
conn_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(conn_string)

@st.cache_data
def load_data(query, params=None):
    with engine.connect() as connection:
        return pd.read_sql(query, connection, params=params)

# Sidebar for user inputs
def sidebar():
    st.sidebar.header("Filters")
    start_date = st.sidebar.date_input("Start date", datetime(2023, 1, 1))
    end_date = st.sidebar.date_input("End date", datetime.today())
    time_frame = st.sidebar.selectbox("Select time frame", ["Daily", "Weekly", "Monthly", "Yearly"])

    return start_date, end_date, time_frame
# Adjusted SQL Queries with placeholders

def get_orders(start_date, end_date):
    orders_query = """
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
    
    # products_query = """
    # SELECT 
    #     p.id AS product_id,
    #     p.vendor_id
    # FROM 
    #     products p;
    # """
    # product_variation_query = """
    # SELECT 
    #     pv.product_id,
    #     pv.price
    # FROM 
    #     product_variation pv;
    # """

    params = {"start_date": start_date, "end_date": end_date}
    return load_data(orders_query, params)
def get_product():
    products_query = """
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
    """
    return load_data(products_query)

def get_product_names():
    product_names_query = """
    SELECT 
        pn.id AS name_id,
        pn.name AS product_name
    FROM 
        product_names pn;
    """
    return load_data(product_names_query)


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

def calculate_total_sales(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name', 'product_name','date', 'total_sales']
    return total_sales
def calculate_total_sales_vendors(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['sales'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    total_sales = merged_data.groupby(['vendor_name',  'time_frame'])['sales'].sum().reset_index()
    total_sales.columns = ['vendor_name','date', 'total_sales']
    return total_sales

def calculate_order_volume(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','product_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name','product_name', 'date', 'order_count']
    return order_volume
def calculate_order_volume_vendor(orders, products, time_frame):
    completed_orders = orders[orders['status'] == 'COMPLETED']
    merged_data = completed_orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    order_volume = merged_data.groupby(['vendor_name','time_frame']).size().reset_index(name='order_count')
    order_volume.columns = ['vendor_name', 'date', 'order_count']
    return order_volume
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

def calculate_average_order_value(orders, products, time_frame):
    # products = products.merge(product_names, on='name_id')
    merged_data = orders.merge(products, on='product_id')
    # merged_data['order_value'] = merged_data['total_amount'/] 
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'product_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','product_name', 'date', 'average_order_value']
    return average_order_value
def calculate_average_order_value_vendor(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data['order_value'] = merged_data['group_price'] * merged_data['quantity']
    merged_data = aggregate_data(merged_data, time_frame)
    average_order_value = merged_data.groupby(['vendor_name', 'time_frame'])['order_value'].mean().reset_index()
    average_order_value.columns = ['vendor_name','date', 'average_order_value']
    return average_order_value

def calculate_fulfillment_time(orders, products, time_frame):
    orders = orders.merge(products, on='product_id')
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['updated_at'] = pd.to_datetime(orders['updated_at'])
    orders['fulfillment_time'] = (orders['updated_at'] - orders['created_at']).dt.total_seconds() / 3600  # in hours
    orders = aggregate_data(orders, time_frame, date_column='created_at')
    fulfillment_time = orders.groupby(['vendor_name','product_name',  'time_frame'])['fulfillment_time'].mean().reset_index()
    return fulfillment_time[['vendor_name', 'product_name','time_frame', 'fulfillment_time']]

def calculate_fulfillment_time_vendor(orders, products, time_frame):
    # products = products.merge(product_names, on='name_id')
    orders = orders.merge(products, on='product_id')
    orders['created_at'] = pd.to_datetime(orders['created_at'])
    orders['updated_at'] = pd.to_datetime(orders['updated_at'])
    orders['fulfillment_time'] = (orders['updated_at'] - orders['created_at']).dt.total_seconds() / 3600  # in hours
    orders = aggregate_data(orders, time_frame, date_column='created_at')
    fulfillment_time = orders.groupby(['vendor_name', 'time_frame'])['fulfillment_time'].mean().reset_index()
    return fulfillment_time[['vendor_name', 'time_frame', 'fulfillment_time']]
def product_popularity(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    product_popularity = merged_data.groupby(['vendor_name', 'product_name','time_frame'])['quantity'].sum().reset_index()
    product_popularity.columns = ['vendor_name','product_name', 'time_frame',  'product_popularity']
    return product_popularity
def product_popularity_vendor(orders, products, time_frame):
    merged_data = orders.merge(products, on='product_id')
    merged_data = aggregate_data(merged_data, time_frame)
    product_popularity = merged_data.groupby(['vendor_name','time_frame'])['quantity'].sum().reset_index()
    product_popularity.columns = ['vendor_name','time_frame',  'product_popularity']
    return product_popularity
