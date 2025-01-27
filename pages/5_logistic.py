import datetime
import pandas as pd
import streamlit as st
import altair as alt
import folium
from streamlit_folium import folium_static
from folium.plugins import MarkerCluster
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
#applying centeralized connection pool
from db_pool import get_conn, release_conn
from plotly.subplots import make_subplots

import matplotlib.pyplot as plt
from st_aggrid import AgGrid, GridOptionsBuilder
from geopy.distance import geodesic

@st.cache_data
def fetch_aggregated_data(start_date, end_date, driver_ids):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Build the new driver condition based on the selected driver IDs
            driver_condition = ""
            params = [start_date, end_date]
            
            if driver_ids:
                driver_condition = "AND d.driver_id = ANY(%s::uuid[])"
                
                params.append(driver_ids)
            
            # Subquery for pre-aggregating distance traveled and drop-offs
            query_routes = f"""
                SELECT 
                    sub.period,
                    sub.driver_id,
                    SUM(sub.total_distance_without_return) AS total_distance_without_return,
                    SUM(sub.total_drop_offs) AS total_drop_offs,
                    SUM(sub.total_operational_capacity_utilized) AS total_operational_capacity_utilized
                FROM (
                    SELECT 
                        date_trunc('day', r.start_time) AS period, 
                        d.driver_id,
                        r.distance AS total_distance_without_return,
                        jsonb_array_length(r.location_ids::jsonb) AS total_drop_offs,
                        r.weight AS total_operational_capacity_utilized
                    FROM routes r
                    JOIN delivery_tracks d ON r.id = d.route_id
                    WHERE date_trunc('day', r.start_time) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                    {driver_condition}
                    GROUP BY period, d.driver_id, r.distance, r.location_ids, r.weight
                ) sub
                GROUP BY sub.period, sub.driver_id
                ORDER BY sub.period;
            """
            cur.execute(query_routes, params)
            data_routes = cur.fetchall()
            colnames_routes = [desc[0] for desc in cur.description]

            df_routes = pd.DataFrame(data_routes, columns=colnames_routes)

            query_orders = f"""
                SELECT 
                    sub.period,
                    sub.driver_id,
                    COUNT(DISTINCT sub.order_id) AS number_of_orders,
                    COUNT(DISTINCT sub.group_cart_id) AS number_of_group_orders,
                    COUNT(DISTINCT sub.personal_cart_id) AS number_of_personal_orders,
                    SUM( CASE WHEN sub.delivered IS NOT NULL THEN 1 ELSE 0 END) AS number_of_delivered_orders
                FROM (
                    SELECT 
                        date_trunc('day', d.in_transit) AS period, 
                        d.driver_id,
                        d.id AS order_id,
                        d.group_cart_id,
                        d.personal_cart_id,
                        d.delivered
                    FROM delivery_tracks d
                    WHERE d.in_transit IS NOT NULL
                    AND date_trunc('day', d.in_transit) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                    {driver_condition}
                    GROUP BY period, d.driver_id, d.id, d.group_cart_id, d.personal_cart_id, d.delivered
                ) sub
                GROUP BY sub.period, sub.driver_id
                ORDER BY sub.period;
            """
            cur.execute(query_orders, params)
            data_orders = cur.fetchall()
            colnames_orders = [desc[0] for desc in cur.description]

            df_orders = pd.DataFrame(data_orders, columns=colnames_orders)
            
            query_delivered_percentage = f"""
                SELECT 
                    date_trunc('day', d.created_at) AS period,
                    d.driver_id,
                    ((SELECT COUNT(id) FROM delivery_tracks WHERE delivered IS NOT NULL AND driver_id = d.driver_id )::float / 
                     NULLIF((SELECT COUNT(id) FROM delivery_tracks WHERE assigned IS NOT NULL AND driver_id = d.driver_id ), 0)) * 100 AS delivered_percentage
                FROM delivery_tracks d
                WHERE date_trunc('day', d.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period, d.driver_id
                ORDER BY period;
            """

            cur.execute(query_delivered_percentage, params)
            data_delivered_percentage = cur.fetchall()
            colnames_delivered_percentage = [desc[0] for desc in cur.description]
            df_delivered_percentage = pd.DataFrame(data_delivered_percentage, columns=colnames_delivered_percentage)
            # query for Delivered Percentage per Driver (DELIVERED VS ASSIGNED) based on the selected ferquency
            
            query_delivered_percentage_each_day = f"""
                WITH daily_counts AS (
                    SELECT 
                        date_trunc('day', d.in_transit) AS period,
                        d.driver_id,
                        COUNT(*) FILTER (WHERE d.delivered IS NOT NULL) AS daily_delivered_count,
                        COUNT(*) FILTER (WHERE d.assigned IS NOT NULL ) AS daily_assigned_count
                    FROM delivery_tracks d
                    WHERE date_trunc('day', d.in_transit) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                    
                    {driver_condition}
                    GROUP BY period, d.driver_id
                )
                SELECT 
                    period,
                    driver_id,
                    (daily_delivered_count::float / NULLIF(daily_assigned_count, 0)) * 100 AS delivered_percentage_per_day
                FROM daily_counts
                ORDER BY period;
            """
            

            cur.execute(query_delivered_percentage_each_day,params)
            data_delivered_percentage_each_day = cur.fetchall()
            colnames_delivered_percentage_each_day = [desc[0] for desc in cur.description]
            df_delivered_percentage_each_day = pd.DataFrame(data_delivered_percentage_each_day, columns=colnames_delivered_percentage_each_day)
            
            query_returned_percentage_each_day = f"""
                WITH daily_counts AS (
                    SELECT 
                        date_trunc('day', d.in_transit) AS period,
                        d.driver_id,
                        COUNT(*) FILTER (WHERE d.status = 'RETURNED') AS daily_returned_count,
                        COUNT(*) FILTER (WHERE d.assigned IS NOT NULL ) AS daily_assigned_count
                    FROM delivery_tracks d
                    WHERE date_trunc('day', d.in_transit) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                    
                    {driver_condition}
                    GROUP BY period, d.driver_id
                )
                SELECT 
                    period,
                    driver_id,
                    (daily_returned_count::float / NULLIF(daily_assigned_count, 0)) * 100 AS returned_percentage_per_day
                FROM daily_counts
                ORDER BY period;
            """
            

            cur.execute(query_returned_percentage_each_day,params)
            data_returned_percentage_each_day = cur.fetchall()
            colnames_returned_percentage_each_day = [desc[0] for desc in cur.description]
            df_returned_percentage_each_day = pd.DataFrame(data_returned_percentage_each_day, columns=colnames_returned_percentage_each_day)

            # Query for top delivery locations by number of deliveries
            query_top_locations = f"""
                SELECT 
                    date_trunc('day', d.created_at) AS period,
                    dl.name,
                    COUNT(*) AS delivery_count,
                    (dl.location[0])::float AS longitude,
                    (dl.location[1])::float AS latitude,
                    STRING_AGG(DISTINCT u.name, ', ') AS driver_names
                FROM delivery_tracks d
                JOIN groups_carts gc ON d.group_cart_id = gc.id
                JOIN orders o ON gc.id = o.groups_carts_id
                JOIN delivery_location dl ON o.location_id = dl.id
                JOIN drivers dr ON d.driver_id = dr.id
                JOIN users u ON dr.user_id = u.id
                WHERE date_trunc('day', d.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period, dl.name, longitude, latitude
                ORDER BY delivery_count DESC
                LIMIT 10;
            """
            
            cur.execute(query_top_locations, params)
            data_top_locations = cur.fetchall()
            colnames_top_locations = [desc[0] for desc in cur.description]

            df_top_locations = pd.DataFrame(data_top_locations, columns=colnames_top_locations)
            df_top_locations['delivery_count'] = df_top_locations['delivery_count'].astype(int)

            # Query for average rating per driver
            query_avg_rating = f"""
                SELECT 
                    date_trunc('day', d.created_at) AS period,
                    d.driver_id,
                    AVG(dr.rating) AS average_rating
                FROM delivery_tracks d
                JOIN routes r ON d.route_id = r.id
                JOIN driver_rating dr ON r.id = dr.route_id
                WHERE date_trunc('day', d.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period, d.driver_id
                ORDER BY period;
            """
            cur.execute(query_avg_rating, params)
            data_avg_rating = cur.fetchall()
            colnames_avg_rating = [desc[0] for desc in cur.description]
            df_avg_rating = pd.DataFrame(data_avg_rating, columns=colnames_avg_rating)

            
            query_avg_capacity = f"""
                SELECT 
                    date_trunc('day', r.start_time) AS period,
                    d.driver_id,
                    AVG(r.weight / v.size) * 100 AS average_capacity_used
                FROM routes r
                JOIN delivery_tracks d ON r.id = d.route_id
                JOIN vehicles v ON d.driver_id = v.driver_id
                WHERE date_trunc('day', r.start_time) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period, d.driver_id, r.weight, v.size
                ORDER BY period;
            """
            cur.execute(query_avg_capacity, params)
            data_avg_capacity = cur.fetchall()
            colnames_avg_capacity = [desc[0] for desc in cur.description]
            df_avg_capacity = pd.DataFrame(data_avg_capacity, columns=colnames_avg_capacity)
            
            query_total_deliveries = f"""
                SELECT  
                    date_trunc('day', d.in_transit) AS period,   
                    d.driver_id,
                    COUNT(*) AS total_deliveries_within_date_range
                FROM delivery_tracks d
                WHERE date_trunc('day', d.in_transit) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period,d.driver_id;
            """

            cur.execute(query_total_deliveries, params)
            data_total_deliveries = cur.fetchall()
            colnames_total_deliveries = [desc[0] for desc in cur.description]

            df_total_deliveries = pd.DataFrame(data_total_deliveries, columns=colnames_total_deliveries)

            # Query for average delivery time
            query_avg_delivery_time = f"""
                SELECT 
                    date_trunc('day', d.in_transit) AS period, 
                    d.driver_id,
                    AVG(EXTRACT(EPOCH FROM (d.delivered - d.in_transit)) / 60) AS average_delivery_time
                FROM delivery_tracks d
                WHERE d.in_transit IS NOT NULL AND d.delivered IS NOT NULL
                AND date_trunc('day', d.in_transit) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                {driver_condition}
                GROUP BY period, d.driver_id
                ORDER BY period;
            """
            cur.execute(query_avg_delivery_time, params)
            data_avg_delivery_time = cur.fetchall()
            colnames_avg_delivery_time = [desc[0] for desc in cur.description]
            df_avg_delivery_time = pd.DataFrame(data_avg_delivery_time, columns=colnames_avg_delivery_time)

            # Merge the dataframes on 'period' and 'driver_id'
            df_combined = pd.merge(df_routes, df_orders, on=['period', 'driver_id'], how='outer', suffixes=('_route', '_order'))
            df_combined = pd.merge(df_combined, df_avg_capacity, on=['period', 'driver_id'], how='left')
            df_combined = pd.merge(df_combined, df_delivered_percentage, on=['period', 'driver_id'], how='left')
            df_combined = pd.merge(df_combined, df_avg_rating, on=['period', 'driver_id'], how='left')
            df_combined = df_combined.merge(df_avg_delivery_time, on=['period', 'driver_id'], how='outer')
            df_combined = pd.merge(df_combined, df_total_deliveries, on=['period', 'driver_id'], how='left')
            df_combined = pd.merge(df_combined, df_delivered_percentage_each_day, on=['period', 'driver_id'], how='left')
            df_combined = pd.merge(df_combined, df_returned_percentage_each_day, on=['period', 'driver_id'], how='left')
            
            df_distance_traveled = df_combined[['period', 'driver_id', 'total_distance_without_return']].drop_duplicates()
            df_total_orders = df_combined[['period', 'driver_id', 'number_of_orders', 'number_of_group_orders', 'number_of_personal_orders','number_of_delivered_orders']].drop_duplicates()
            df_drop_offs = df_combined[['period', 'driver_id', 'total_drop_offs']].drop_duplicates()
            df_operational_capacity = df_combined[['period', 'driver_id', 'total_operational_capacity_utilized']].drop_duplicates()
            df_avg_capacity = df_combined[['period', 'driver_id', 'average_capacity_used']].drop_duplicates()
            df_delivered_percentage = df_combined[['period', 'driver_id', 'delivered_percentage']].drop_duplicates()
            df_avg_rating = df_combined[['period', 'driver_id', 'average_rating']].drop_duplicates()
            df_avg_delivery_time = df_combined[['period', 'driver_id', 'average_delivery_time']].drop_duplicates()
            df_total_deliveries = df_combined[['period', 'driver_id', 'total_deliveries_within_date_range']].drop_duplicates()
            df_delivered_percentage_each_day = df_combined[['period', 'driver_id', 'delivered_percentage_per_day']].drop_duplicates()
            df_returned_percentage_each_day = df_combined[['period', 'driver_id', 'returned_percentage_per_day']].drop_duplicates()

            return df_distance_traveled, df_total_orders, df_drop_offs, df_operational_capacity, df_avg_capacity, df_delivered_percentage, df_top_locations, df_avg_rating, df_total_deliveries, df_delivered_percentage_each_day, df_avg_delivery_time, df_returned_percentage_each_day

    finally:
        if conn:
            release_conn(conn)

@st.cache_data
def fetch_driver_names_and_ids():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Query for driver names and IDs
            query_driver_names_and_ids = """
                SELECT u.name AS driver_name, d.id AS driver_id
                FROM drivers d
                JOIN users u ON d.user_id = u.id
                ORDER BY u.name;
            """
            cur.execute(query_driver_names_and_ids)
            drivers = cur.fetchall()
            
            # Creating a dictionary to map driver IDs to names
            driver_dict = {row[1]: row[0] for row in drivers}
            
            return driver_dict
    finally:
        if conn:
            release_conn(conn)
    
@st.cache_data
def fetch_report_data(start_date, end_date):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            #query for delivered vs returned orders
            query1 = """SELECT 
                            DATE_TRUNC('day', in_transit) AS delivery_date,
                            COUNT(id) FILTER (WHERE in_transit IS NOT NULL) AS orders_total,
                            COUNT(id) FILTER (WHERE delivered IS NOT NULL) AS delivered_orders,
                            COUNT(id) FILTER (WHERE status = 'RETURNED') AS returned_orders,
                        ((SELECT COUNT (dt1.id) FROM  delivery_tracks AS dt1
                            JOIN delivery_tracks AS dt2 ON dt1.personal_cart_id = dt2.personal_cart_id
                            WHERE dt1.status = 'RETURNED' AND dt2.delivered IS NOT NULL) + (SELECT COUNT (dt1.id) FROM  delivery_tracks AS dt1
                            JOIN delivery_tracks AS dt2 ON dt1.group_cart_id = dt2.group_cart_id
                            WHERE dt1.status = 'RETURNED' AND dt2.delivered IS NOT NULL)) AS returned_then_delivered
            FROM 
                            delivery_tracks
                        WHERE 
                            in_transit BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                        GROUP BY 
                            delivery_date
                        ORDER BY 
                            delivery_date;
            """

            # Query for returned orders per reason
            query2 = """SELECT 
                DATE_TRUNC('day', in_transit) AS date,
                return_reason,
                COUNT(*) AS count
            FROM 
                delivery_tracks
            WHERE 
                status = 'RETURNED' AND
                created_at BETWEEN %s AND %s + interval '1 day' - interval '1 second'
            GROUP BY 
                created_at,date, return_reason
            ORDER BY 
                created_at,date, return_reason;
            """
            cur.execute(query1, (start_date, end_date))
            delivered_vs_returned_data = cur.fetchall()

            cur.execute(query2, (start_date, end_date))
            returned_orders_reason_data = cur.fetchall()

        # Convert the fetched data into DataFrames
        delivered_vs_returned_df = pd.DataFrame(delivered_vs_returned_data, columns=['delivery_date','orders_total','delivered_orders', 'returned_orders','returned_then_delivered'])
        returned_orders_reason_df = pd.DataFrame(returned_orders_reason_data, columns=['created_at', 'return_reason', 'count'])
        
        # Calculate percentages
        delivered_vs_returned_df['delivered_orders_percentage'] = (delivered_vs_returned_df['delivered_orders'] / delivered_vs_returned_df['orders_total']) * 100
        delivered_vs_returned_df['returned_orders_percentage'] = (delivered_vs_returned_df['returned_orders'] / delivered_vs_returned_df['orders_total']) * 100
      
        return delivered_vs_returned_df, returned_orders_reason_df
    finally:
        if conn:
            release_conn(conn)
@st.cache_data
def fetch_delivery_data(start_date, end_date, driver_ids, frequency):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            driver_condition = ""
            params = [start_date, end_date]

            if driver_ids:
                driver_condition = "AND d.driver_id = ANY(%s::uuid[])"
                params.append(driver_ids)
            
            # Adjust the DATE_TRUNC based on the frequency
            if frequency == "Daily":
                date_trunc = "day"
            elif frequency == "Weekly":
                date_trunc = "week"
            elif frequency == "Monthly":
                date_trunc = "month"

            # Queries for different time frames
            query_delivered = f"""
                SELECT
                    DATE_TRUNC('{date_trunc}', dt.assigned) AS assigned_time,
                    dt.driver_id,
                    COUNT(dt.id) AS total_delivered,
                    CASE
                        WHEN AGE(dt.delivered, dt.assigned) <= INTERVAL '24 hours' THEN '24 Hours'
                        WHEN AGE(dt.delivered, dt.assigned) > INTERVAL '24 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '48 hours' THEN '48 Hours'
                        WHEN AGE(dt.delivered, dt.assigned) > INTERVAL '48 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '72 hours' THEN '72 Hours'
                        WHEN AGE(dt.delivered, dt.assigned) > INTERVAL '72 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '96 hours' THEN '4_days'
                        WHEN AGE(dt.delivered, dt.assigned) > INTERVAL '96 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '120 hours' THEN '5_days'
                        WHEN AGE(dt.delivered, dt.assigned) > INTERVAL '120 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '144 hours' THEN '6_days'
                        ELSE 'More than 6 Days'
                    END AS delivery_time_frame
                FROM delivery_tracks dt
                WHERE dt.assigned IS NOT NULL
                AND dt.delivered IS NOT NULL
                AND dt.assigned BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                {driver_condition}
                GROUP BY DATE_TRUNC('{date_trunc}', dt.assigned), dt.driver_id, delivery_time_frame
                ORDER BY assigned_time DESC
            """

            query_unpicked_1 = f"""
                SELECT
                    DATE_TRUNC('{date_trunc}', dt.assigned) AS assigned_time,
                    dt.driver_id,
                    COUNT(dt.id) AS total_unpicked
                FROM delivery_tracks dt
                WHERE dt.assigned IS NOT NULL
                AND dt.in_transit IS NULL
                AND dt.assigned BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                {driver_condition}
                GROUP BY DATE_TRUNC('{date_trunc}', dt.assigned), dt.driver_id
                ORDER BY assigned_time DESC
            """
            query_unpicked = f"""SELECT 
                    DATE_TRUNC('{date_trunc}', dt.assigned) AS assigned_time,
                    dt.driver_id,
                    COUNT(DISTINCT (o.location_id,gc.group_id)) AS total_unpicked
                FROM 
                    delivery_tracks dt
                JOIN 
                    groups_carts gc ON gc.id = dt.group_cart_id
                LEFT JOIN 
                    orders o ON o.groups_carts_id = gc.id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
                WHERE 
                    dt.assigned IS NOT NULL
                    AND dt.in_transit IS NULL
                    AND dt.assigned BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                    {driver_condition}
                GROUP BY 
                    DATE_TRUNC('{date_trunc}', dt.assigned), dt.driver_id
                ORDER BY 
                    assigned_time DESC;

            """
            query_unpicked_personal = f"""SELECT 
                    DATE_TRUNC('{date_trunc}', dt.assigned) AS assigned_time,
                    dt.driver_id,
                    COUNT(DISTINCT (o.location_id, pci.cart_id)) AS total_unpicked
                FROM 
                    delivery_tracks dt
                JOIN personal_cart_items pci ON
                    pci.id = dt.personal_cart_id
                JOIN carts c ON
                    c.id = pci.cart_id
                LEFT JOIN 
                    orders o ON o.personal_cart_id = c.id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
                WHERE 
                    dt.assigned IS NOT NULL
                    AND dt.in_transit IS NULL
                    AND dt.assigned BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                    {driver_condition}
                GROUP BY 
                    DATE_TRUNC('{date_trunc}', dt.assigned), dt.driver_id
                ORDER BY 
                    assigned_time DESC;

            """

            query_unassigned_1 = f"""
                SELECT
                    DATE_TRUNC('{date_trunc}', dt.created_at) AS accepted_time,
                    COUNT(dt.id) AS total_unassigned
                FROM delivery_tracks dt
                WHERE dt.assigned IS NULL
                AND dt.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY DATE_TRUNC('{date_trunc}', dt.created_at)
                ORDER BY accepted_time DESC
            """
            query_unassigned = f"""SELECT 
                DATE_TRUNC('{date_trunc}', dt.created_at) AS accepted_time,
                gc.id AS group_id,
                COUNT(DISTINCT (o.location_id, gc.group_id)) AS total_unassigned
            FROM 
                delivery_tracks dt
            JOIN 
                groups_carts gc ON gc.id = dt.group_cart_id
            LEFT JOIN 
                orders o ON o.groups_carts_id = gc.id
                AND o.status = 'COMPLETED'
                AND o.deleted_at IS NULL
            WHERE 
                dt.assigned IS NULL
                AND dt.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                DATE_TRUNC('{date_trunc}', dt.created_at),gc.id
            ORDER BY 
                accepted_time DESC;
                """
            query_unassigned_personal = f"""SELECT 
                DATE_TRUNC('{date_trunc}', dt.created_at) AS accepted_time,
                --gc.id AS group_id,
                COUNT(DISTINCT (o.location_id, pci.cart_id)) AS total_unassigned
            FROM 
                delivery_tracks dt
            JOIN personal_cart_items pci ON
                pci.id = dt.personal_cart_id
            JOIN carts c ON
                c.id = pci.cart_id
            LEFT JOIN 
                orders o ON o.personal_cart_id = c.id
                AND o.status = 'COMPLETED'
                AND o.deleted_at IS NULL
            WHERE 
                dt.assigned IS NULL
                AND dt.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                DATE_TRUNC('{date_trunc}', dt.created_at)
            ORDER BY 
                accepted_time DESC;
                """

            cur.execute(query_delivered, params)
            data_delivered = cur.fetchall()
            colnames_delivered = [desc[0] for desc in cur.description]
            df_delivered = pd.DataFrame(data_delivered, columns=colnames_delivered)
            
            cur.execute(query_unpicked_1, params)
            data_unpicked_1 = cur.fetchall()
            colnames_unpicked_1 = [desc[0] for desc in cur.description]
            df_unpicked_1 = pd.DataFrame(data_unpicked_1, columns=colnames_unpicked_1)

            cur.execute(query_unassigned_1, params)
            data_unassigned_1 = cur.fetchall()
            colnames_unassigned_1 = [desc[0] for desc in cur.description]
            df_unassigned_1 = pd.DataFrame(data_unassigned_1, columns=colnames_unassigned_1)

            cur.execute(query_unpicked, params)
            data_unpicked = cur.fetchall()
            colnames_unpicked = [desc[0] for desc in cur.description]
            df_unpicked = pd.DataFrame(data_unpicked, columns=colnames_unpicked)

            cur.execute(query_unassigned, params)
            data_unassigned = cur.fetchall()
            colnames_unassigned = [desc[0] for desc in cur.description]
            df_unassigned = pd.DataFrame(data_unassigned, columns=colnames_unassigned)
            
            cur.execute(query_unassigned_personal, params)
            data_unassigned_personal = cur.fetchall()
            colnames_unassigned_personal = [desc[0] for desc in cur.description]
            df_unassigned_personal = pd.DataFrame(data_unassigned_personal, columns=colnames_unassigned_personal)
         
            cur.execute(query_unpicked_personal, params)
            data_unpicked_personal = cur.fetchall()
            colnames_unpicked_personal = [desc[0] for desc in cur.description]
            df_unpicked_personal = pd.DataFrame(data_unpicked_personal, columns=colnames_unpicked_personal)
        
            
            return df_delivered, df_unpicked, df_unassigned, df_unpicked_1, df_unassigned_1,  df_unpicked_personal,  df_unassigned_personal
        
        
    finally:
        if conn:
            release_conn(conn)


@st.cache_data
def fetch_summary_data(start_date, end_date, driver_ids, frequency):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            driver_condition = ""
            params = [start_date, end_date]

            if driver_ids:
                driver_condition = "AND d.driver_id = ANY(%s::uuid[])"
                params.append(driver_ids)

            # Adjust the DATE_TRUNC based on the frequency
            if frequency == "Daily":
                date_trunc = "day"
            elif frequency == "Weekly":
                date_trunc = "week"
            elif frequency == "Monthly":
                date_trunc = "month"

            query_summary = f"""
                SELECT 
                    DATE_TRUNC('{date_trunc}', dt.assigned) as assigned_day,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) <= INTERVAL '24 hours') as delivered_24_hr,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) > INTERVAL '24 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '48 hours') as delivered_2_days,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) > INTERVAL '48 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '72 hours') as delivered_3_days,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) > INTERVAL '72 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '96 hours') as delivered_4_days,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) > INTERVAL '96 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '120 hours') as delivered_5_days,
                    COUNT(dt.id) FILTER (WHERE AGE(dt.delivered, dt.assigned) > INTERVAL '120 hours' AND AGE(dt.delivered, dt.assigned) <= INTERVAL '144 hours') as delivered_6_days,
                    COUNT(dt.id) as total_assigned,
                    -- Additional counts for each status
                    COUNT(dt.id) FILTER (WHERE dt.status = 'REJECTED_BY_VENDOR') as rejected_by_vendor,
                    COUNT(dt.id) FILTER (WHERE dt.status = 'RETURNED') as returned,
                    COUNT(dt.id) FILTER (WHERE dt.status ='IN_TRANSIT') as in_transit,
                    COUNT(dt.id) FILTER (WHERE dt.status ='IN_PROGRESS')as in_progress,
                    --COUNT(dt.id) FILTER (WHERE dt.assigned IS NULL) as total_unassigned
                     -- New count for returned orders that were delivered again
                    COUNT(dt.id) FILTER (
                        WHERE dt.status = 'RETURNED' 
                        AND dt.return_reason IS NOT NULL
                        AND EXISTS (
                            SELECT 1 FROM delivery_tracks dt2 
                            WHERE dt2.id = dt.id 
                            AND dt2.delivered IS NOT NULL
                            AND dt2.created_at > dt.created_at -- ensuring it's delivered after being returned
                        )
                    ) AS returned_and_delivered_again
                FROM delivery_tracks dt
                WHERE dt.assigned IS NOT NULL
                AND dt.delivered IS NOT NULL
                AND dt.assigned BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                {driver_condition}
                GROUP BY assigned_day
                ORDER BY assigned_day DESC;
            """
            cur.execute(query_summary, params)
            data_summary = cur.fetchall()
            colnames_summary = [desc[0] for desc in cur.description]

            df_summary = pd.DataFrame(data_summary, columns=colnames_summary)
            return df_summary  
    finally:
        if conn:
            release_conn(conn)


def aggregate_by_week(data, metrics_info):
    if 'delivery_date' not in data.columns and 'created_at' not in data.columns:
        raise ValueError("The input data must contain 'delivery_date' or 'created_at' columns.")

    # Choose the correct date column
    date_col = 'delivery_date' if 'delivery_date' in data.columns else 'created_at'
    data[date_col] = pd.to_datetime(data[date_col])
    data = data[data[date_col] >= '2023-10-01']  # Ensure data is only from October 2023 onwards

    # Adjust to ensure the week starts on Wednesday and ends on Tuesday
    data[date_col] -= pd.to_timedelta((data[date_col].dt.weekday + 1) % 7, unit='d')

    # Initialize the full weeks range
    start_date = pd.to_datetime('2023-10-01') - pd.to_timedelta((pd.to_datetime('2023-10-01').weekday() + 1) % 7, unit='d')
    current_date = pd.to_datetime('today')
    full_weeks = pd.date_range(start=start_date, end=current_date, freq='W-WED')

    # Resample data to weekly, ending on Wednesday
    if 'return_reason' in data.columns:
        aggregated_data = data.groupby(['return_reason', pd.Grouper(key=date_col, freq='W-WED')]).agg(metrics_info).reset_index()
    else:
        aggregated_data = data.set_index(date_col).resample('W-WED').agg(metrics_info).reset_index()

    # Merge with full weeks range to ensure all weeks are present
    full_data = pd.DataFrame({date_col: full_weeks})
    aggregated_data = pd.merge(full_data, aggregated_data, on=date_col, how='left').fillna(0)
    

    # Ensure data is sorted by the actual date before creating week ranges
    aggregated_data = aggregated_data.sort_values(date_col)

    # Create week ranges for display
    aggregated_data['week_start'] = aggregated_data[date_col].dt.strftime('%b %d')
    aggregated_data['week_end'] = (aggregated_data[date_col] + pd.DateOffset(days=6)).dt.strftime('%b %d')
    aggregated_data['week'] = aggregated_data['week_start'] + ' to ' + aggregated_data['week_end']
    aggregated_data = aggregated_data.rename(columns={'week': 'frequency'})

    return aggregated_data


def aggregate_by_day(data, metrics_info, start_date, end_date):
    if 'delivery_date' not in data.columns and 'created_at' not in data.columns:
        raise ValueError("The input data must contain 'delivery_date' or 'created_at' columns.")

    date_col = 'delivery_date' if 'delivery_date' in data.columns else 'created_at'
    data[date_col] = pd.to_datetime(data[date_col])
    data = data[(data[date_col] >= start_date) & (data[date_col] <= end_date)]

    if 'return_reason' in data.columns:
        aggregated_data = data.groupby(['return_reason', pd.Grouper(key=date_col, freq='D')]).agg(metrics_info).reset_index()
    else:
        data = data.set_index(date_col)
        aggregated_data = data.resample('D').agg(metrics_info).reset_index()

    full_days = pd.date_range(start=start_date, end=end_date, freq='D')
    full_data = pd.DataFrame({date_col: full_days})
    for metric in metrics_info.keys():
        full_data[f'{metric}_agg'] = 0
    aggregated_data = pd.merge(full_data, aggregated_data, on=date_col, how='left').fillna(0)

    aggregated_data = aggregated_data.sort_values(date_col)
    aggregated_data['day'] = aggregated_data[date_col].dt.strftime('%b %d')
    aggregated_data = aggregated_data.rename(columns={'day': 'frequency'})

    return aggregated_data


def aggregate_by_month(data, metrics_info):
    if 'delivery_date' not in data.columns and 'created_at' not in data.columns:
        raise ValueError("The input data must contain 'delivery_date' or 'created_at' columns.")

    date_col = 'delivery_date' if 'delivery_date' in data.columns else 'created_at'
    data[date_col] = pd.to_datetime(data[date_col])
    data = data[data[date_col] >= '2023-10-01']

    if 'return_reason' in data.columns:
        aggregated_data = data.groupby(['return_reason', pd.Grouper(key=date_col, freq='M')]).agg(metrics_info).reset_index()
    else:
        data = data.set_index(date_col)
        aggregated_data = data.resample('M').agg(metrics_info).reset_index()

    start_date = pd.to_datetime('2023-10-01')
    current_date = pd.to_datetime('today')
    full_months = pd.date_range(start=start_date, end=current_date, freq='M')
    full_data = pd.DataFrame({date_col: full_months})
    for metric in metrics_info.keys():
        full_data[f'{metric}_agg'] = 0
    aggregated_data = pd.merge(full_data, aggregated_data, on=date_col, how='left').fillna(0)

    aggregated_data = aggregated_data.sort_values(date_col)
    aggregated_data['month'] = aggregated_data[date_col].dt.strftime('%B')
    aggregated_data = aggregated_data.rename(columns={'month': 'frequency'})

    return aggregated_data



def compute_percentage_change(aggregated_data, metric):
    # Print available columns for debugging
    print("Available columns:", aggregated_data.columns)

    if metric not in aggregated_data.columns:
        raise KeyError(f"Metric '{metric}' not found in aggregated data columns.")

    aggregated_data['previous_week'] = aggregated_data[metric].shift(1)
    aggregated_data['previous_2_week'] = aggregated_data[metric].shift(2)
   
    aggregated_data['percentage_change'] = (aggregated_data['previous_week']- aggregated_data['previous_2_week'] ) / \
                                            aggregated_data['previous_2_week'].replace(0, np.nan) * 100
    aggregated_data['percentage_change'] = aggregated_data['percentage_change'].fillna(0)

    return aggregated_data
# Function to show trend view
def show_trend_view(metric_data_dict, frequency, start_date, end_date):
    single_chart_metrics = ['count']
    dual_chart_metrics = [
    
        ['delivered_orders_percentage','returned_orders_percentage']
    ]
    metrics_info = {
        'Delivered vs Returned': {
            'delivered_orders_percentage': 'mean',
            'returned_orders_percentage': 'mean'
        },
        'Returned Orders per Reasons': {
            'count': 'sum',
        }
    }
    color_map = {
        'delivered_orders_percentage': 'green',
        'returned_orders_percentage': 'red',
        'count': 'red'
    }

    for data_name, metric_data in metric_data_dict.items():
        if metric_data.empty:
            st.markdown(
                f"<div style='background-color: #f8d7da; padding: 20px; border-radius: 5px;'><h3>No data available for {data_name} in the selected date range.</h3></div>",
                unsafe_allow_html=True
            )
            continue

        if 'delivery_date' not in metric_data.columns and 'created_at' not in metric_data.columns:
            st.markdown(
                f"<div style='background-color: #f8d7da; padding: 20px; border-radius: 5px;'><h3>{data_name} does not contain 'delivery_date' or 'created_at' columns.</h3></div>",
                unsafe_allow_html=True
            )
            continue

        for metric in single_chart_metrics:
            if metric not in metrics_info[data_name]:
                continue
            metrics_info_local = {metric: metrics_info[data_name][metric]}
            if frequency == 'Weekly':
                aggregated_data = aggregate_by_week(metric_data, metrics_info_local)
            elif frequency == 'Monthly':
                aggregated_data = aggregate_by_month(metric_data, metrics_info_local)
            else:
                aggregated_data = aggregate_by_day(metric_data, metrics_info_local, start_date, end_date)

            aggregated_data = compute_percentage_change(aggregated_data, metric)

            if len(aggregated_data) < 2:
                st.markdown(f"""
                <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                    <h3>{metric.replace("_", " ").title()} ({data_name})</h3>
                    <p>Not enough data to display trend</p>
                </div>
                <div>
                </div>
                """, unsafe_allow_html=True)
                continue

            last_week = aggregated_data.iloc[-1]
            previous_week = aggregated_data.iloc[-2]
            percentage_change_last_week = last_week['percentage_change']

            st.markdown(f"""
            <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                <h3>{metric.replace("_", " ").title()} ({data_name})</h3>
                <p>From {previous_week['frequency']} to {last_week['frequency']}</p>
                <p>Percentage Change: {percentage_change_last_week:.2f}%</p>
            </div>
            """, unsafe_allow_html=True)

            if 'return_reason' in aggregated_data.columns:
                unique_reasons = aggregated_data['return_reason'].unique()
                reason_color_map = {reason: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)] for i, reason in enumerate(unique_reasons)}
                # Calculate dynamic height
                base_height = 600
                extra_height_per_reason = 20
                dynamic_height = base_height + len(unique_reasons) * extra_height_per_reason
                fig = px.line(aggregated_data, x='frequency', y=metric, color='return_reason',
                              title=f'({data_name}) {frequency.title()} Trend',
                              color_discrete_map=reason_color_map)
            else:
                fig = px.line(aggregated_data, x='frequency', y=metric,
                              title=f'{metric.replace("_", " ").title()} ({data_name}) {frequency.title()} Trend')
            dynamic_height = 600
            fig.update_layout(
                autosize=False,
                width=1600,
                height=dynamic_height,
                margin=dict(l=40, r=40, b=150, t=100),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=0.88,
                    xanchor="right",
                    x=1
            )
            )
            fig.update_xaxes(
                tickangle=300,
                tickformat='%d %b',
                tickmode='linear',
                dtick=1,
                title=frequency,
                automargin=True
            )
            fig.update_yaxes(title=f"{metric.replace('_', ' ').title()}")
            fig.update_traces(mode='lines+markers')
            st.plotly_chart(fig)

        for metrics_pair in dual_chart_metrics:
            metric1, metric2 = metrics_pair
            if metric1 not in metrics_info[data_name] or metric2 not in metrics_info[data_name]:
                continue
            metrics_info_local = {metric1: metrics_info[data_name][metric1], metric2: metrics_info[data_name][metric2]}
            if frequency == 'Weekly':
                aggregated_data = aggregate_by_week(metric_data, metrics_info_local)
            elif frequency == 'Monthly':
                aggregated_data = aggregate_by_month(metric_data, metrics_info_local)
            else:
                aggregated_data = aggregate_by_day(metric_data, metrics_info_local, start_date, end_date)

            metric1_column = f'{metric1}'
            metric2_column = f'{metric2}'

            aggregated_data1 = compute_percentage_change(aggregated_data, metric1_column)
            aggregated_data2 = compute_percentage_change(aggregated_data, metric2_column)

            if len(aggregated_data) < 2:
                st.markdown(f"""
                <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                    <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()} ({data_name})</h3>
                    <p>Not enough data to display trend</p>
                </div>
                """, unsafe_allow_html=True)
                continue

            last_week = aggregated_data.iloc[-1]
            previous_week = aggregated_data.iloc[-2]
            percentage_change_last_week_metric1 = last_week['percentage_change']
            percentage_change_last_week_metric2 = last_week['percentage_change']

            st.markdown(f"""
            <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()} ({data_name})</h3>
                <p>From {previous_week['frequency']} to {last_week['frequency']}</p>
                <p>{metric1.replace("_", " ").title()}  Change: {percentage_change_last_week_metric1:.2f}%</p>
                <p>{metric2.replace("_", " ").title()}  Change: {percentage_change_last_week_metric2:.2f}%</p>
            </div>
            """, unsafe_allow_html=True)

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=aggregated_data1['frequency'], y=aggregated_data1[metric1_column], mode='lines+markers',
                                     name=metric1.replace("_", " ").title(), line=dict(color=color_map.get(metric1, 'green'))))
            fig.add_trace(go.Scatter(x=aggregated_data2['frequency'], 
                                     y=aggregated_data2[metric2_column],
                                     mode='lines+markers',
                                     name=metric2.replace("_", " ").title(), 
                                     line=dict(color=color_map.get(metric2, 'red'))))

            fig.update_layout(
                autosize=False,
                width=1600,
                height=400,
                margin=dict(l=40, r=40, b=200, t=40),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=0.99,
                    xanchor="right",
                    x=1
                )
            )
            fig.update_xaxes(
                tickangle=300,
                tickformat='%d %b',
                tickmode='linear',
                dtick=1,
                title=frequency,
                automargin=True
            )
            fig.update_yaxes(title=f"{metric1.replace('_', ' ').title()} and {metric2.replace('_', ' ').title()}")
            st.plotly_chart(fig)

  
# Streamlit application code
st.title('Logistics KPI Dashboard')

# Sidebar filters
default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Fetch driver names and IDs for the dropdown
driver_dict = fetch_driver_names_and_ids()
driver_names = list(driver_dict.values())
driver_names.insert(0, "All")  # Add "All" option at the beginning

# Driver name filter
selected_driver_names = st.sidebar.multiselect("Select Driver", driver_names, default=["All"])

# Convert driver names to IDs
selected_driver_ids = [key for key, value in driver_dict.items() if value in selected_driver_names and value != "All"]

# Frequency filter
frequency = st.sidebar.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])


def aggregate_locations(df, max_distance_km=3):
    aggregated_data = []

    while not df.empty:
        base_location = df.iloc[0]
        base_coords = (base_location['latitude'], base_location['longitude'])

        close_locations = df[df.apply(
            lambda row: geodesic(base_coords, (row['latitude'], row['longitude'])).km <= max_distance_km,
            axis=1
        )]

        aggregated_row = {
            'name': ', '.join(close_locations['name']),
            'latitude': close_locations['latitude'].mean(),
            'longitude': close_locations['longitude'].mean(),
            'delivery_count': close_locations['delivery_count'].sum(),
            'driver_names': ', '.join(sorted(set(', '.join(close_locations['driver_names']).split(', '))))
        }

        aggregated_data.append(aggregated_row)
        df = df.drop(close_locations.index)

    return pd.DataFrame(aggregated_data)

def visualize_top_locations_on_heatmap(df):
    if df.empty:
        st.markdown("No data available for the selected filters.")
        return

    st.subheader(f"Top Delivery Locations with Markers" " "
                f"from {start_date.date()} to {end_date.date()}")
    st.write("This provides information on the top delivery locations by the number of deliveries, including the locations, and a comma-separated list of driver names who made deliveries to that location during the specified date range.")

    # Create a base map
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8)

    # Add marker cluster for better visualization
    marker_cluster = MarkerCluster().add_to(m)

    # Add markers for each location
    for index, row in df.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        delivery_count = row['delivery_count']

        # Create marker with popup showing delivery count
        popup_content = f"<strong>Location:</strong> {row['name']}<br><strong>Delivery Count:</strong> {delivery_count}<br><strong>Driver Names:</strong> {row['driver_names']}"
        marker = folium.Marker(
            location=(lat, lon),
            popup=popup_content,
            icon=folium.DivIcon(html=f"""
                <div style="font-family: sans-serif; color: white; background-color: rgba(0, 0, 0, 0.6); padding: 2px 20px; border-radius: 3px;">
                    {delivery_count}
                </div>""")
        )
        marker.add_to(marker_cluster)

    # Display map
    folium_static(m)

    
def process_dataframe(df, frequency):
    if df.empty:
        return df
    
    # Convert 'period' to datetime
    df['period'] = pd.to_datetime(df['period'])
    
    # Extract date from datetime
    df['date'] = df['period'].dt.date
    
    # Add 'frequency' column based on the selected frequency
    if frequency == 'Weekly':
        df['frequency'] = df['period'].dt.to_period('W').apply(lambda r: r.start_time.date())
    elif frequency == 'Monthly':
        df['frequency'] = df['period'].dt.to_period('M').apply(lambda r: r.start_time.date())
    else:
        df['frequency'] = df['date']
    
    return df
def convert_columns_to_numeric(df, columns):
    if df.empty:
        return df
    
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
    
    return df

# Define the columns to convert for each DataFrame
columns_dict = {
    'df_operational_capacity': ['total_operational_capacity_utilized'],
    'df_distance_traveled': ['total_distance_without_return'],
    'df_total_orders': [
        'number_of_orders',
        'number_of_group_orders',
        'number_of_personal_orders',
        'number_of_delivered_orders'
    ],
    'df_drop_offs': ['total_drop_offs'],
    'df_avg_capacity': ['average_capacity_used'],
    'df_delivered_percentage': ['delivered_percentage'],
    'df_avg_rating': ['average_rating'],
    'df_total_deliveries': ['total_deliveries_within_date_range'],
    'df_delivered_percentage_each_day': ['delivered_percentage_per_day'],
    'df_returned_percentage_each_day': ['returned_percentage_per_day']
}
def aggregate_and_index(df, group_by_columns, agg_dict, index_start=1):
    if df.empty:
        return pd.DataFrame()
    
    # Group by specified columns and aggregate
    aggregated_df = df.groupby(group_by_columns).agg(agg_dict).reset_index()
    
    # Reset index to start from a specific value
    aggregated_df.index = range(index_start, len(aggregated_df) + index_start)
    
    return aggregated_df

# Define aggregation parameters for each DataFrame
aggregation_params = {
    'df_distance_traveled': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'total_distance_without_return': 'sum'}
    },
    'df_total_orders': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {
            'number_of_orders': 'sum',
            'number_of_group_orders': 'sum',
            'number_of_personal_orders': 'sum',
            'number_of_delivered_orders': 'sum'
        }
    },
    'df_drop_offs': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'total_drop_offs': 'sum'}
    },
    'df_operational_capacity': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'total_operational_capacity_utilized': 'sum'}
    },
    'df_avg_capacity': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'average_capacity_used': 'mean'}
    },
    'df_delivered_percentage': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'delivered_percentage': 'mean'}
    },
    'df_avg_rating': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'average_rating': 'mean'}
    },
    'df_total_deliveries': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'total_deliveries_within_date_range': 'sum'}
    },
    'df_delivered_percentage_each_day': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'delivered_percentage_per_day': 'mean'}
    },
    'df_returned_percentage_each_day': {
        'group_by_columns': ['frequency', 'driver_name'],
        'agg_dict': {'returned_percentage_per_day': 'mean'}
    }
}

def visualize_data(df, date_column, value_column, index_column, column_order=None):
  
    # Visualizes a DataFrame using AgGrid.
    if df.empty:
        st.markdown(f"""
        <div style="background-color: #f8d7da; padding: 20px; border-radius: 5px;">
            <h3>No data available for the selected criteria.</h3>
        </div>
        """, unsafe_allow_html=True)
        return
    
    try:
        # Ensure the date column is converted to datetime
        df[date_column] = pd.to_datetime(df[date_column])

        # Convert the date column to string for pivoting
        df[date_column] = df[date_column].dt.strftime('%Y-%m-%d')

        # Pivot the DataFrame
        df_pivot = df.pivot(index=index_column, columns=date_column, values=value_column).reset_index()
        
        # Reorder the columns based on the desired order (if provided)
        if column_order:
            # Ensure index_column is the first column in the order
            column_order = [index_column] + [col for col in column_order if col in df_pivot.columns]
            df_pivot = df_pivot[column_order]

        # Creating a GridOptionsBuilder instance
        gb = GridOptionsBuilder.from_dataframe(df_pivot)
        gb.configure_default_column(editable=True, groupable=True)
        gb.configure_column(index_column, pinned='left')  # Pinning the index column

        # Building the grid options
        gridOptions = gb.build()

        # Display the table with AgGrid
        AgGrid(
            df_pivot,
            gridOptions=gridOptions,
            enable_enterprise_modules=True,
            height=400,
            width='100%',
        )
    except Exception as e:
        st.error(f"An error occurred: {e}")
# Fetch the aggregated data
df_distance_traveled, df_total_orders, df_drop_offs, df_operational_capacity,df_avg_capacity,df_delivered_percentage,df_top_locations,df_avg_rating,df_total_deliveries, df_delivered_percentage_each_day,df_avg_delivery_time,df_returned_percentage_each_day = fetch_aggregated_data(start_date, end_date, selected_driver_ids)

# Replace driver IDs with names in dataframes
def map_driver_id(df, driver_dict):
    if df.empty:
        return df
    
    # Map driver IDs to names
    df['driver_name'] = df['driver_id'].map(driver_dict)
    
    # Drop the 'driver_id' column
    df.drop(columns=['driver_id'], inplace=True)
    
    return df

# Apply the function to each DataFrame
df_distance_traveled = map_driver_id(df_distance_traveled, driver_dict)
df_total_orders = map_driver_id(df_total_orders, driver_dict)
df_drop_offs = map_driver_id(df_drop_offs, driver_dict)
df_operational_capacity = map_driver_id(df_operational_capacity, driver_dict)
df_avg_capacity = map_driver_id(df_avg_capacity, driver_dict)
df_delivered_percentage = map_driver_id(df_delivered_percentage, driver_dict)
df_avg_rating = map_driver_id(df_avg_rating, driver_dict)
df_total_deliveries = map_driver_id(df_total_deliveries, driver_dict)
df_delivered_percentage_each_day = map_driver_id(df_delivered_percentage_each_day, driver_dict)
df_returned_percentage_each_day = map_driver_id(df_returned_percentage_each_day, driver_dict)
# Fetch data
df_delivered, df_unpicked, df_unassigned, df_unpicked_1, df_unassigned_1,  df_unpicked_personal,  df_unassigned_personal  = fetch_delivery_data(start_date, end_date, selected_driver_ids,frequency)
# Apply the function to each DataFrame
df_delivered = map_driver_id(df_delivered, driver_dict)

df_unpicked = map_driver_id(df_unpicked, driver_dict)

df_unpicked_1 = map_driver_id(df_unpicked_1, driver_dict)

df_unpicked_personal = map_driver_id(df_unpicked_personal, driver_dict)


# Fetch summary data
df_summary = fetch_summary_data(start_date, end_date, selected_driver_ids,frequency)
df_summary = df_summary.rename(columns={
         
        "total_assigned" : "total orders assigned",
        "rejected_by_vendor": "orders rejected by vendors",
        "returned": "returned orders",
        "in_transit": "orders accepted by vendors",
        "in_progress": "orders pending vendor acceptance"
    })

tab1, tab2 = st.tabs(["Logistics performance", "Delivery performance"])

# Apply filters button
if st.sidebar.button("Filter"):
    with tab1:
        try:# Process each DataFrame with the function
            df_distance_traveled = process_dataframe(df_distance_traveled, frequency)
            df_total_orders = process_dataframe(df_total_orders, frequency)
            df_drop_offs = process_dataframe(df_drop_offs, frequency)
            df_operational_capacity = process_dataframe(df_operational_capacity, frequency)
            df_avg_capacity = process_dataframe(df_avg_capacity, frequency)
            df_delivered_percentage = process_dataframe(df_delivered_percentage, frequency)
            df_top_locations = process_dataframe(df_top_locations, frequency)
            df_avg_rating = process_dataframe(df_avg_rating, frequency)
            df_delivered_percentage_each_day = process_dataframe(df_delivered_percentage_each_day, frequency)
            df_returned_percentage_each_day = process_dataframe(df_returned_percentage_each_day, frequency)
            df_total_deliveries = process_dataframe(df_total_deliveries, frequency)
                    
            try:
                # Apply the function to each DataFrame
                df_operational_capacity = convert_columns_to_numeric(df_operational_capacity, columns_dict['df_operational_capacity'])
                df_distance_traveled = convert_columns_to_numeric(df_distance_traveled, columns_dict['df_distance_traveled'])
                df_total_orders = convert_columns_to_numeric(df_total_orders, columns_dict['df_total_orders'])
                df_drop_offs = convert_columns_to_numeric(df_drop_offs, columns_dict['df_drop_offs'])
                df_avg_capacity = convert_columns_to_numeric(df_avg_capacity, columns_dict['df_avg_capacity'])
                df_delivered_percentage = convert_columns_to_numeric(df_delivered_percentage, columns_dict['df_delivered_percentage'])
                df_avg_rating = convert_columns_to_numeric(df_avg_rating, columns_dict['df_avg_rating'])
                df_total_deliveries = convert_columns_to_numeric(df_total_deliveries, columns_dict['df_total_deliveries'])
                df_delivered_percentage_each_day = convert_columns_to_numeric(df_delivered_percentage_each_day, columns_dict['df_delivered_percentage_each_day'])
                df_returned_percentage_each_day = convert_columns_to_numeric(df_returned_percentage_each_day, columns_dict['df_returned_percentage_each_day'])
                
            except Exception as e:
                st.error(f"No data available for the selected date range. Please select a different date range : {e}")
            
            
            # Apply the aggregation function to each DataFrame
            aggregated_distance_traveled = aggregate_and_index(df_distance_traveled, **aggregation_params['df_distance_traveled'])
            aggregated_total_orders = aggregate_and_index(df_total_orders, **aggregation_params['df_total_orders'])
            aggregated_drop_offs = aggregate_and_index(df_drop_offs, **aggregation_params['df_drop_offs'])
            aggregated_operational_capacity = aggregate_and_index(df_operational_capacity, **aggregation_params['df_operational_capacity'])
            aggregated_avg_capacity = aggregate_and_index(df_avg_capacity, **aggregation_params['df_avg_capacity'])
            aggregated_delivered_percentage = aggregate_and_index(df_delivered_percentage, **aggregation_params['df_delivered_percentage'])
            aggregated_avg_rating = aggregate_and_index(df_avg_rating, **aggregation_params['df_avg_rating'])
            aggregated_total_deliveries = aggregate_and_index(df_total_deliveries, **aggregation_params['df_total_deliveries'])
            aggregated_delivered_percentage_each_day = aggregate_and_index(df_delivered_percentage_each_day, **aggregation_params['df_delivered_percentage_each_day'])
            aggregated_returned_percentage_each_day = aggregate_and_index(df_returned_percentage_each_day, **aggregation_params['df_returned_percentage_each_day'])


            # Further aggregate for delivered percentage if not empty
            if not df_delivered_percentage.empty:
                final_aggregated_data = aggregated_delivered_percentage.groupby('driver_name').agg({'delivered_percentage': 'mean'}).reset_index()
                final_aggregated_data.index = range(1, len(final_aggregated_data) + 1)
            else:
                final_aggregated_data = pd.DataFrame()
            
        except Exception as e:
            st.error(f"No data available for the selected date range. Please select a different date range : {e}")
        
        try:
            if not df_top_locations.empty:
                
                # Convert delivery_count to numeric (if not already)
                df_top_locations['delivery_count'] = pd.to_numeric(df_top_locations['delivery_count'], errors='coerce')

                # Group by frequency, name, latitude, and longitude to combine driver names and sum delivery counts
                aggregated_top_locations = df_top_locations.groupby(
                    ['frequency', 'name', 'latitude', 'longitude']
                ).agg({
                    'driver_names': lambda x: ', '.join(sorted(set(driver for drivers in x for driver in drivers.split(', ')))),
                    'delivery_count': 'sum'
                }).reset_index()

                # Further aggregate to get a single value per location across all dates
                final_aggregated_top_locations = aggregated_top_locations.groupby(
                    ['name']
                ).agg({
                    'driver_names': lambda x: ', '.join(sorted(set(driver for drivers in x for driver in drivers.split(', ')))),
                    'delivery_count': 'sum'
                }).reset_index()
                aggregated_top_locations_2 = aggregate_locations(df_top_locations)
                
                visualize_top_locations_on_heatmap(aggregated_top_locations_2)

                # Define the columns to keep
                desired_columns = ['name', 'driver_names', 'delivery_count']

                # Select only the desired columns
                df_oc = aggregated_top_locations_2[desired_columns]

                # Rename columns for display
                df_tl = df_oc.rename(columns={'frequency': 'Date', 'name': 'Location', 'driver_names': 'Driver Names', 'delivery_count': 'Total Delivery Count'})

                # Sort the DataFrame by 'Total Delivery Count' in descending order
                df_tl = df_tl.sort_values(by='Total Delivery Count', ascending=False)

                # Reset the index
                df_tl = df_tl.reset_index(drop=True)

                # Display the sorted DataFrame
                st.write(df_tl)
               
            else:
                st.markdown(f"""
                <div style="background-color: #f8d7da; padding: 20px; border-radius: 5px;">
                    <h3>No data available for the selected date range.</h3>
                    <p>Please select a different date range.</p>      
                </div>
                """, unsafe_allow_html=True)
            st.header("Driver Efficiency")
            st.subheader(f"Operational Capacity Utilized"" "
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("This KPI measures the total weight of goods delivered within the selected date range. It's calculated by summing up the weight of goods for each route within the selected date range. The data is displayed in a line chart with 'Date' on the x-axis and 'Total Weight (kg)' on the y-axis.")
            
            df_oc = aggregated_operational_capacity.rename(columns={'frequency': 'Date','driver_name':'Driver name', 'total_operational_capacity_utilized': 'Total Weight (kg)'})
           
            visualize_data(df_oc,
                           date_column='Date',
                           index_column= 'Driver name',
                           value_column='Total Weight (kg)',
                           )
            
            if not aggregated_operational_capacity.empty:
                line_chart_operational_capacity = alt.Chart(aggregated_operational_capacity).mark_line(point=True).encode(
                    x='frequency:T',
                    y='total_operational_capacity_utilized:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'total_operational_capacity_utilized:Q']
                ).properties(
                    title='Operational Capacity Utilized'
                ).interactive()
                st.altair_chart(line_chart_operational_capacity, use_container_width=True)
                
            
            st.subheader(f"Average Capacity Used"" "
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("The average percentage of the vehicle capacity utilized by each driver during the specified date range.This is calculated by dividing the total weight of deliveries by the size of the vehicle assigned to the driver.")
            df_av = aggregated_avg_capacity.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'average_capacity_used': 'Average Capacity Used'})
            visualize_data(df_av,
                date_column='Date',
                index_column='Driver name',
                value_column='Average Capacity Used',
            )
         
            if not aggregated_avg_capacity.empty:
                avg_capacity_chart = alt.Chart(aggregated_avg_capacity).mark_line(point=True).encode(
                    x='frequency:T',
                    y='average_capacity_used:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'average_capacity_used:Q']
                ).properties(
                    title='Average Capacity Used (%)'
                ).interactive()
                st.altair_chart(avg_capacity_chart, use_container_width=True)
            st.header("Delivery Performance")
            # Total orders
            st.subheader(f"Total Orders"" "
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("This KPI measures the total number of orders placed within the selected date range. It also showa two categories of orders: 'Total Group Orders' and 'Total Personal Orders'. The data is displayed in a line chart with 'Date' on the x-axis and 'Total Orders' on the y-axis.")
            df_to = aggregated_total_orders.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'number_of_orders': 'Total Orders', 'number_of_delivered_orders':'Total Delivered Orders','number_of_group_orders': 'Total Group Orders','number_of_personal_orders': 'Total Personal Orders'})
            st.write(df_to)
            
            if not aggregated_total_orders.empty:
                line_chart_total_orders = alt.Chart(aggregated_total_orders).mark_line(point=True).encode(
                    x='frequency:T',
                    y='number_of_orders:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'number_of_orders:Q']
                ).properties(
                    title='Total Orders'
                ).interactive()
                st.altair_chart(line_chart_total_orders, use_container_width=True)
            # Number of Drop-offs
            st.subheader(f"Number of Drop-offs"" "
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("This KPI measures the total number of delivery locations visited within the selected date range. It's calculated by summing up the number of locations for each route within the selected date range. The data is displayed in a line chart with 'Date' on the x-axis and 'Total Drop-offs' on the y-axis.")
            df_do = aggregated_drop_offs.rename(columns={'frequency': 'Date', 'driver_name':'Driver name','total_drop_offs': 'Total Drop-offs'})
            
            visualize_data(df_do,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Total Drop-offs',
                           )

            if not aggregated_drop_offs.empty:
                line_chart_drop_offs = alt.Chart(aggregated_drop_offs).mark_line(point=True).encode(
                    x='frequency:T',
                    y='total_drop_offs:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'total_drop_offs:Q']
                ).properties(
                    title='Total Drop Offs'
                ).interactive()
                st.altair_chart(line_chart_drop_offs, use_container_width=True) 
            # Distance Traveled Without Return
            st.subheader(f"Distance Traveled Without Return"" "
                      f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("This KPI measures the total distance traveled by vehicles during deliveries, excluding any return trips. It's calculated by summing up the distances for each route within the selected date range. The data is displayed in a line chart with 'Date' on the x-axis and 'Total Distance (km)' on the y-axis.")
            df_dt = aggregated_distance_traveled.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'total_distance_without_return': 'Total Distance (km)'})
            visualize_data(df_dt,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Total Distance (km)',
                           )
            if not aggregated_distance_traveled.empty:
                line_chart_distance_traveled = alt.Chart(aggregated_distance_traveled).mark_line(point=True).encode(
                    x='frequency:T',
                    y='total_distance_without_return:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'total_distance_without_return:Q']
                ).properties(
                    title='Distance Traveled'
                ).interactive()
                
                st.altair_chart(line_chart_distance_traveled, use_container_width=True)
           
            # Visualization for Total Deliveries This Month
            st.subheader(f"Total Deliveries by each Driver" " " 
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")

            df_td = aggregated_total_deliveries.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'total_deliveries_within_date_range': 'Total Deliveries'})
            visualize_data(df_td,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Total Deliveries',
                           )
            total_deliveries_chart = alt.Chart(aggregated_total_deliveries).mark_line(point=True).encode(
                x='frequency:T',
                y='total_deliveries_within_date_range:Q',
                color='driver_name:N',
                tooltip=['driver_name:N', 'total_deliveries_within_date_range:Q']
            ).properties(
                title='Total Deliveries'
            ).interactive()
            st.altair_chart(total_deliveries_chart, use_container_width=True) 
            
            st.subheader("Delivered Percentage per Driver (DELIVERED VS ASSIGNED) based on the selected ferquency") 
            st.write("The percentage of orders delivered by each driver out of the total number of assigned orders during the specified date range. This is computed based on a selected frequency basis.")
            df_dp = aggregated_delivered_percentage_each_day.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'delivered_percentage_per_day': 'Delivered Percentage'})
            visualize_data(df_dp,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Delivered Percentage',
                           )
            if not df_delivered_percentage_each_day.empty:
                delivered_percentage_each_day_chart = alt.Chart(aggregated_delivered_percentage_each_day).mark_line(point=True).encode(
                    x='frequency:T',
                    y=alt.Y('delivered_percentage_per_day:Q', scale=alt.Scale(domain=[0, 100]), title='Delivered Percentage (%)'),
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'delivered_percentage_per_day:Q']
                ).properties(
                    title='Delivered Percentage'
                ).interactive()
            
                st.altair_chart(delivered_percentage_each_day_chart, use_container_width=True) 
            
            st.subheader("Returned Percentage per Driver (RETURNED VS ASSIGNED) based on the selected ferquency") 
            st.write("The percentage of orders returned by each driver out of the total number of assigned orders during the specified date range. This is computed based on a selected frequency basis.")
            df_rp = aggregated_returned_percentage_each_day.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'returned_percentage_per_day': 'Returned Percentage'})
            visualize_data(df_rp,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Returned Percentage',
                           )
            if not df_returned_percentage_each_day.empty:
                returned_percentage_each_day_chart = alt.Chart(aggregated_returned_percentage_each_day).mark_line(point=True).encode(
                    x='frequency:T',
                    y=alt.Y('returned_percentage_per_day:Q', scale=alt.Scale(domain=[0, 100]), title='Returned Percentage (%)'),
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'returned_percentage_per_day:Q']
                ).properties(
                    title='Returned Percentage'
                ).interactive()
            
                st.altair_chart(returned_percentage_each_day_chart, use_container_width=True) 
            
            
            st.subheader("Delivered Percentage (Overall Efficiency) per Driver (DELIVERED VS ASSIGNED)") 
            st.write("This metric shows the percentage of deliveries successfully completed by the driver over the entire dataset. It provides an aggregate view of the driver’s delivery efficiency, helping you assess overall performance without breaking it down by individual days or frequency.")
            
            df_dp = final_aggregated_data.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'delivered_percentage': 'Delivered Percentage'})
            st.write(df_dp)
            if not final_aggregated_data.empty:
                delivered_percentage_chart = alt.Chart(final_aggregated_data).mark_bar(size=5).encode(
                    x='driver_name:N',
                    y=alt.Y('delivered_percentage:Q', scale=alt.Scale(domain=[0, 100]), title='Delivered Percentage (%)'),
                    color='driver_name:N', 
                    tooltip=['driver_name:N', 'delivered_percentage:Q']
                ).properties(
                    title='Delivered Percentage'
                ).interactive()
                st.altair_chart(delivered_percentage_chart, use_container_width=True)
                
            st.header("Driver Quality")
            st.subheader(f"Average Rating"" "
                         f"from {start_date.date()} to {end_date.date()} with {frequency} frequency")
            st.write("The average rating received by each driver from users based on their delivery experience during the specified date range.")
            aggregated_avg_rating['frequency'] = pd.to_datetime(aggregated_avg_rating['frequency'])
            aggregated_avg_rating['frequency'] = aggregated_avg_rating['frequency'].dt.strftime('%Y-%m-%d')
            df_a = aggregated_avg_rating.rename(columns={'frequency': 'Date','driver_name':'Driver name',  'average_rating': 'Average Rating'})
            
            visualize_data(df_a,
                           date_column='Date',
                           index_column='Driver name',
                           value_column='Average Rating',
                          )
            
            if not aggregated_avg_rating.empty:
                avg_rating_chart = alt.Chart(aggregated_avg_rating).mark_line(point=True).encode(
                    x='frequency:T',
                    y='average_rating:Q',
                    color='driver_name:N',
                    tooltip=['driver_name:N', 'average_rating:Q']
                ).properties(
                    title='Average Rating'
                ).interactive()
                st.altair_chart(avg_rating_chart, use_container_width=True) 

            
        except Exception as e:
            st.markdown(f"""
            <div style="background-color: #f8d7da; padding: 20px; border-radius: 5px;">
                <h3>No data available for the selected date range.</h3>
                <p>Please select a different date range.</p>
                <p><strong>Error details:</strong> {e}</p>
            </div>
            """, unsafe_allow_html=True)
    with tab2:
       
        # Reshape data for line chart visualization
        df_summary_melted = df_summary.melt(id_vars=["assigned_day"], 
                                    value_vars=["total orders assigned", "delivered_24_hr", "delivered_2_days", "delivered_3_days", "delivered_4_days", "delivered_5_days", "delivered_6_days","orders rejected by vendors", "returned orders", "orders accepted by vendors", "orders pending vendor acceptance","returned_and_delivered_again"],
                                    var_name="delivery_time_frame", value_name="count")
    
        # Visualization for the summary
        st.subheader("Order Delivery Summary")
        st.write("""
                 Delivery Time Metrics:
                    This metrics tracks the delivery performance by calculating how long it takes for orders to be delivered from the time they are assigned. Key delivery time frames include:

                    1. ** 24 Hours **: Orders delivered within 24 hours of being assigned.
                    2. ** 2 Days **: Orders delivered between 24 and 48 hours of being assigned.
                    3. ** 3 Days **: Orders delivered between 48 and 72 hours of being assigned.
                    4. ** 4-6 Days **: Orders delivered between 4 and 6 days of being assigned.
                    5. ** More than 6 Days **: Orders taking longer than 6 days to be delivered.
                 """)
 
        visualize_data(df_summary_melted, date_column="assigned_day", value_column="count", index_column="delivery_time_frame")

        fig_summary = px.line(
            df_summary_melted,
            x="assigned_day",
            y="count",
            color="delivery_time_frame",
            title="Total Assigned and Delivered Orders Over Time",
            labels={"assigned_day": "Date", "count": "Number of Orders", "delivery_time_frame": "Delivery Time Frame"},
            markers=True
        )

        st.plotly_chart(fig_summary)
        # Visualization
        st.subheader("Order Deliveries with in different time period.")

        # Define a list of delivery time frames
        time_frames = ["24 Hours", "48 Hours", "72 Hours","4_days", "5_days","6_days", "More than 6 Days"]

        # Iterate over each time frame and create an expandable section
        for time_frame in time_frames:
            with st.expander(f"Orders Delivered within {time_frame}", expanded=False):
                # Filter the data for the current time frame
                df_time_frame = df_delivered[df_delivered['delivery_time_frame'] == time_frame]
                # Aggregate the data by assigned_time (if not already done in SQL)
                df_time_frame = df_time_frame.groupby('assigned_time').agg({'total_delivered': 'sum'}).reset_index()
                
                # Create a line chart for the current time frame
                fig_delivered = px.line(
                    df_time_frame, 
                    x='assigned_time', 
                    y='total_delivered', 
                    title=f"Orders Delivered within {time_frame}",
                    markers=True
                )
                
                # Display the chart
                st.plotly_chart(fig_delivered)
        
        # Unpicked Orders Visualization
        st.subheader("Total Unpicked Orders by Driver and Date")
        st.write("This metric tracks the total number of unpicked deliveries assigned to drivers within the selected time frame. Unpicked deliveries refer to orders that have been assigned to drivers but have not yet begun transit.  The data is grouped by the assignment date and driver, offering insights into individual driver performance and overall operational efficiency.")
        visualize_data(df_unpicked_1, date_column="assigned_time", value_column="total_unpicked", index_column="driver_name")
        fig_unpicked = px.line(
            df_unpicked_1, 
            x='assigned_time', 
            y='total_unpicked', 
            color='driver_name',  # Use driver_name for color differentiation
            title="Unpicked Orders",
            hover_data=['driver_name'],  # Display driver names in hover
            markers=True
        )
        st.plotly_chart(fig_unpicked)

        st.subheader("Unpicked Group Orders Locations by Driver and Date")
        st.write("This query tracks the efficiency of drivers by counting the number of unique locations where Group orders have been assigned but not yet picked up. The data is grouped by driver and date, helping identify potential delays in the delivery process and monitor overall driver performance.")
        visualize_data(df_unpicked, date_column="assigned_time", value_column="total_unpicked", index_column="driver_name")
        fig_unpicked = px.line(
            df_unpicked, 
            x='assigned_time', 
            y='total_unpicked', 
            color='driver_name',  # Use driver_name for color differentiation
            title="Unpicked Group Orders",
            hover_data=['driver_name'],  # Display driver names in hover
            markers=True
        )
        st.plotly_chart(fig_unpicked)

        st.subheader("Unpicked Personal Orders Locations by Driver and Date")
        st.write("This query tracks the efficiency of drivers by counting the number of unique locations where Personal orders have been assigned but not yet picked up. The data is grouped by driver and date, helping identify potential delays in the delivery process and monitor overall driver performance.")
        visualize_data(df_unpicked_personal, date_column="assigned_time", value_column="total_unpicked", index_column="driver_name")
        fig_unpicked_personal = px.line(
            df_unpicked_personal, 
            x='assigned_time', 
            y='total_unpicked', 
            color='driver_name',  # Use driver_name for color differentiation
            title="Unpicked Personal Orders",
            hover_data=['driver_name'],  # Display driver names in hover
            markers=True
        )
        st.plotly_chart(fig_unpicked_personal)
        # Unassigned Orders Visualization

        st.subheader("Total Unassigned Orders")
        st.write("This metric tracks the number of unique locations with orders that were created but never assigned to a driver. It highlights potential gaps in resource management or bottlenecks in the assignment process, which could lead to delayed deliveries or unfulfilled orders.")
        fig_unassigned_1 = px.line(
            df_unassigned_1, 
            x='accepted_time', 
            y='total_unassigned', 
            title="Unassigned Orders",
            hover_data=['total_unassigned'],  # Display driver names in hover
            markers = True
        )
        st.plotly_chart(fig_unassigned_1)

        st.subheader("Unassigned Group Orders")
        st.write("This query tracks the number of unique locations with unassigned orders grouped by group cart ID and date. It helps identify gaps in order assignment for group deliveries, ensuring that no locations are overlooked in the delivery process.")
        fig_unassigned = px.line(
            df_unassigned, 
            x='accepted_time', 
            y='total_unassigned', 
            title="Unassigned Group Orders",
            hover_data=['total_unassigned'],  # Display driver names in hover
            markers = True
        )
        st.plotly_chart(fig_unassigned)

        st.subheader("Unassigned Personal Orders")
        st.write("This query tracks the number of unique locations with unassigned personal orders grouped by personalcart ID and date. It helps identify gaps in order assignment for group deliveries, ensuring that no locations are overlooked in the delivery process.")
        fig_unassigned_personal = px.line(
            df_unassigned_personal, 
            x='accepted_time', 
            y='total_unassigned', 
            title="Unassigned Personal Orders",
            hover_data=['total_unassigned'],  # Display driver names in hover
            markers = True
        )
        st.plotly_chart(fig_unassigned_personal)
        st.subheader("Delivered vs Returned orders ")
        delivered_vs_returned_df,returned_orders_reason_df=fetch_report_data(start_date, end_date)
        
        data_frames = {
            'Delivered vs Returned': delivered_vs_returned_df,
            'Returned Orders per Reasons': returned_orders_reason_df
        }
        st.write(delivered_vs_returned_df[['delivery_date', 'returned_then_delivered']])
        show_trend_view(data_frames, frequency, start_date, end_date)
        
        
        
        
