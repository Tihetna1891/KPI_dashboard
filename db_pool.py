# db_pool.py
import psycopg2
from psycopg2 import pool
import streamlit as st

# Initialize connection pool with a maximum of 4 connections
@st.cache_resource
def get_connection_pool():
    return pool.SimpleConnectionPool(1, 4, dsn=st.secrets["url"])

conn_pool = get_connection_pool()

def get_conn():
    try:
        conn = conn_pool.getconn()
        if conn:
            return conn
    except Exception as e:
        st.error(f"Error getting connection: {e}")
    return None

def release_conn(conn):
    try:
        if conn:
            conn_pool.putconn(conn)
    except Exception as e:
        st.error(f"Error releasing connection: {e}")


