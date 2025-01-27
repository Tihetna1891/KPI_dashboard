import streamlit as st 
import pandas as pd 
import datetime
from db_pool import get_conn, release_conn

# function to fetch data 
@st.cache_data
def fetch_aggregated_data(start_date, end_date, frequency):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            date_trunc = {
                "Daily": "day",
                "Weekly": "week",
                "Monthly": "month"
            }[frequency]
            
           
            params = [start_date, end_date]
            
            query_combined = f"""SELECT 
                DATE_TRUNC('{date_trunc}', pr.created_at)::date AS period, 
                p.id AS product_id, 
                pn.name AS product_name, 
                AVG(pr.rating) AS average_rating,
                COUNT(pr.id) AS review_count
            FROM 
                products p
            JOIN 
                product_ratings pr ON p.id = pr.product_id
            JOIN 
                product_names pn ON p.name_id = pn.id
            WHERE 
                pr.deleted_at IS NULL 
                AND pr.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                period, p.id, pn.name
            ORDER BY 
                average_rating ASC,   -- Worst average rating first
                review_count DESC      -- Among products with the same rating, prioritize those with more reviews
            LIMIT 10;

            """
        
            cur.execute(query_combined,params)
            data_combined=cur.fetchall()
            colnames_combined =  [desc[0] for desc in cur.description]
            df_combined = pd.DataFrame(data_combined, columns =colnames_combined)
            
            # Product Rating Distribution
            query_rating_dist = f"""SELECT 
                DATE_TRUNC('{date_trunc}', created_at)::date AS period,
                rating, 
                COUNT(*) AS count
            FROM 
                product_ratings
            WHERE 
                deleted_at IS NULL
                AND created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                rating, period
            ORDER BY 
                rating;
            """
            cur.execute(query_rating_dist, params)
            data_rating_dist = cur.fetchall()
            colnames_rating_dist = [desc[0] for desc in cur.description]

            df_rating_dist = pd.DataFrame(data_rating_dist, columns=colnames_rating_dist)
            # Products with the Most Reviews
            query_most_review = f"""SELECT 
                    DATE_TRUNC('{date_trunc}', pr.created_at)::date AS period,
                    p.id AS product_id, 
                    pn.name AS product_name, 
                    COUNT(pr.id) AS review_count
                FROM 
                    products p
                JOIN 
                    product_ratings pr ON p.id = pr.product_id
                JOIN 
                    product_names pn ON p.name_id = pn.id
                WHERE 
                    pr.deleted_at IS NULL 
                    AND pr.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY 
                    period, p.id, pn.name
                ORDER BY 
                    review_count ASC
                LIMIT 10;
            """
            cur.execute(query_most_review, params)
            data_most_review = cur.fetchall()
            colnames_most_review = [desc[0] for desc in cur.description]
            
            df_most_review = pd.DataFrame(data_most_review,columns = colnames_most_review)
            # Products with the Highest Rating Variability / inconsistancy
            query_high_variablity = f"""SELECT 
                    DATE_TRUNC('{date_trunc}', pr.created_at)::date AS period,
                    p.id AS product_id, 
                    pn.name AS product_name, 
                    STDDEV(pr.rating) AS rating_stddev
                FROM 
                    products p
                JOIN 
                    product_ratings pr ON p.id = pr.product_id
                JOIN 
                    product_names pn ON p.name_id = pn.id
                WHERE 
                    pr.deleted_at IS NULL 
                    AND pr.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY 
                    period,p.id, pn.name
                ORDER BY 
                    rating_stddev DESC;
                --LIMIT 10;
            """
            cur.execute(query_high_variablity,params)
            data_high_variablity = cur.fetchall()
            colnames_high_variablity = [desc[0] for desc in cur.description]
            
            df_high_variablity = pd.DataFrame(data_high_variablity,columns = colnames_high_variablity)
            
            # Product Performance Over Time
            query_performance = f"""SELECT
                DATE_TRUNC('{date_trunc}', pr.created_at)::date AS period, 
                p.id AS product_id, 
                pn.name AS product_name, 
                AVG(pr.rating) AS average_rating
            FROM 
                products p
            JOIN 
                product_ratings pr ON p.id = pr.product_id
            JOIN 
                product_names pn ON p.name_id = pn.id
            WHERE 
                pr.deleted_at IS NULL 
                AND pr.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                period, p.id, pn.name
            ORDER BY 
                average_rating DESC;
            """
            cur.execute(query_performance,params)
            data_performance = cur.fetchall()
            colnames_performance = [desc[0] for desc in cur.description]
            
            df_performance = pd.DataFrame(data_performance,columns = colnames_performance)
            
            # Top Products by Vendor
            query_by_vendor = f"""SELECT 
                DATE_TRUNC('{date_trunc}', pr.created_at)::date AS period,
                v.id AS vendor_id, 
                v.name AS vendor_name, 
                p.id AS product_id, 
                pn.name AS product_name, 
                AVG(pr.rating) AS average_rating
            FROM 
                vendors v
            JOIN 
                products p ON v.id = p.vendor_id
            JOIN 
                product_ratings pr ON p.id = pr.product_id
            JOIN 
                product_names pn ON p.name_id = pn.id
            WHERE 
                pr.deleted_at IS NULL 
                AND pr.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY 
                period,v.id, v.name, p.id, pn.name
            ORDER BY 
                average_rating ASC
            LIMIT 10;
            """
            cur.execute(query_by_vendor,params)
            data_by_vendor = cur.fetchall()
            colnames_by_vendor = [desc[0] for desc in cur.description]
            
            df_by_vendor = pd.DataFrame(data_by_vendor, columns = colnames_by_vendor)
        
            return df_rating_dist, df_most_review, df_high_variablity, df_performance, df_by_vendor, df_combined
        
    finally:
        if conn:
            release_conn(conn)

default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.datetime.today())
start_date = pd.to_datetime(start_date).date()
end_date = pd.to_datetime(end_date).date()
frequency = st.sidebar.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])
df_rating_dist, df_most_review, df_high_variablity, df_performance, df_by_vendor, df_combined = fetch_aggregated_data(start_date,end_date,frequency)


df_rating_dist=df_rating_dist.drop(columns=['product_id'], errors='ignore')
df_most_review=df_most_review.drop(columns=['product_id'], errors='ignore')
df_high_variablity=df_high_variablity.drop(columns=['product_id'], errors='ignore')
df_performance=df_performance.drop(columns=['product_id'], errors='ignore')
df_by_vendor=df_by_vendor.drop(columns=['product_id','vendor_id'], errors='ignore')
df_combined = df_combined.drop(columns=['product_id'], errors='ignore')

st.subheader(f"Worst Performing Products by Average Rating and Review Volume {frequency} report from {start_date} to {end_date}")
st.write("This KPI identifies the products with the lowest average customer ratings while prioritizing those with the highest number of reviews. By focusing on these products, the metric highlights items that not only perform poorly in customer satisfaction but also attract significant attention (high in review counts), making them critical targets for quality improvement initiatives. This KPI helps in pinpointing products that may require immediate attention to improve overall customer experience and satisfaction.")
df_combined.index = df_combined.index + 1
st.write(df_combined)

st.subheader(f"Top worst Products by Vendor {frequency} report from {start_date} to {end_date}")
st.write("This metric Identify which vendors have the least-rated products, providing insight into vendor performance.")
df_by_vendor.index = df_by_vendor.index + 1
st.write(df_by_vendor)

st.subheader(f"Product Rating Distribution  {frequency} report from {start_date} to {end_date}")
st.write("This metric Show the distribution of product ratings to understand how customers are rating products in general.")
df_rating_dist.index = df_rating_dist.index + 1
st.write(df_rating_dist)

st.subheader(f"Products with the Highest Rating Variability / inconsistancy  {frequency} report from {start_date} to {end_date}")
st.write("This metric Identify products that have the most variability in ratings (high standard deviation), which might indicate inconsistency in quality or customer experience.")
df_high_variablity.index = df_high_variablity.index + 1
st.write(df_high_variablity)

st.subheader(f"Product Performance Over Time {frequency} report from {start_date} to {end_date}")
st.write("This metric Analyze how the average rating of products has changed over time.")
df_performance.index = df_performance.index + 1
st.write(df_performance)


        



    

