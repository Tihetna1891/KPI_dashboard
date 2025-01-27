import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime,timedelta
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
from db_pool import get_conn, release_conn
import numpy as np

# Function to fetch and aggregate data
@st.cache_data
def fetch_aggregated_data(start_date, end_date):
    conn = get_conn()
    
    try:
        with conn.cursor() as cur:
            
            query_first = """
           WITH aggregated_groups AS (
    SELECT
        DATE(g.created_at) AS group_created_date,
        g.id AS group_id,
        g.created_by AS group_leader,
        gd.max_group_member AS max_group_member,
        g.status,
        LEAST(GREATEST(EXTRACT(EPOCH FROM (g.updated_at - g.created_at)) / 3600, 0), 24) AS completion_duration_hours
    FROM
        groups g
    JOIN
        group_deals gd ON g.group_deals_id = gd.id
    WHERE
        g.created_at BETWEEN %s AND %s + interval '1 day' - interval '1 second'
),
new_group_leaders_cte AS (
    SELECT 
        g1.created_by AS group_leader,
        MIN(g1.created_at)::DATE AS first_group_date
    FROM 
        groups g1
    GROUP BY 
        g1.created_by
),
group_stats AS (
    SELECT
        ag.group_created_date,
        ag.group_leader,
        ag.status,
        ag.max_group_member,
        ag.completion_duration_hours,
        COUNT(*) FILTER (WHERE ag.status = 'COMPLETED') AS completed_groups,
        COUNT(*) FILTER (WHERE ag.status = 'FAILED') AS failed_groups,
        COUNT(DISTINCT ag.group_id) AS number_of_orders,
        CASE 
            WHEN ngl.first_group_date = ag.group_created_date THEN 1
            ELSE 0
        END AS is_new_group_leader
    FROM
        aggregated_groups ag
    LEFT JOIN
        new_group_leaders_cte ngl ON ag.group_leader = ngl.group_leader
    GROUP BY
        ag.group_created_date, ag.group_leader, ag.status, ag.max_group_member, ag.completion_duration_hours, ngl.first_group_date
)
SELECT
    group_created_date,
    group_leader,
    status,
    is_new_group_leader,
    COUNT(DISTINCT group_leader) AS unique_group_leaders,
    SUM(completed_groups) AS completed_groups,
    SUM(failed_groups) AS failed_groups,
    SUM(completed_groups) + SUM(failed_groups) AS total_groups,
    AVG(completion_duration_hours) AS average_completion_duration,
    AVG(max_group_member) AS average_group_size,
    SUM(number_of_orders) AS number_of_orders,
    COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN group_leader END) AS unique_group_leaders_completed,
    COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN group_leader END) AS unique_group_leaders_failed,
    COUNT(DISTINCT CASE WHEN is_new_group_leader = 1 THEN group_leader END) AS new_group_leaders,
    SUM(CASE WHEN is_new_group_leader = 1 AND status = 'COMPLETED' THEN 1 ELSE 0 END) AS new_completed_groups,
    SUM(CASE WHEN is_new_group_leader = 1 AND status = 'FAILED' THEN 1 ELSE 0 END) AS new_failed_groups,
    CASE
        WHEN COUNT(DISTINCT group_leader) > 0 THEN CAST(SUM(completed_groups) AS FLOAT) / COUNT(DISTINCT group_leader)
        ELSE 0.0
    END AS unique_group_leaders_success_rate,
    CASE
        WHEN COUNT(DISTINCT group_leader) > 0 THEN CAST(SUM(failed_groups) AS FLOAT) / COUNT(DISTINCT group_leader)
        ELSE 0.0
    END AS unique_group_leaders_failure_rate,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN is_new_group_leader = 1 THEN group_leader END) > 0 THEN CAST(SUM(CASE WHEN is_new_group_leader = 1 AND status = 'COMPLETED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(DISTINCT CASE WHEN is_new_group_leader = 1 THEN group_leader END)
        ELSE 0.0
    END AS new_group_leaders_success_rate,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN is_new_group_leader = 1 THEN group_leader END) > 0 THEN CAST(SUM(CASE WHEN is_new_group_leader = 1 AND status = 'FAILED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(DISTINCT CASE WHEN is_new_group_leader = 1 THEN group_leader END)
        ELSE 0.0
    END AS new_group_leaders_failure_rate,
    CASE
        WHEN SUM(completed_groups + failed_groups) > 0 THEN CAST(SUM(completed_groups) AS FLOAT) / (SUM(completed_groups) + SUM(failed_groups))
        ELSE 0.0
    END AS success_rate,
    CASE
        WHEN SUM(completed_groups + failed_groups) > 0 THEN CAST(SUM(failed_groups) AS FLOAT) / (SUM(completed_groups) + SUM(failed_groups))
        ELSE 0.0
    END AS failure_rate
FROM
    group_stats

GROUP BY
    group_created_date, group_leader, status,is_new_group_leader;


            """
            cur.execute(query_first,(start_date, end_date))
            data_first = cur.fetchall()
            colnames_first = [desc[0] for desc in cur.description]
            df_first = pd.DataFrame(data_first, columns=colnames_first)
            
            # Filter data based on start_date and end_date
            df_first['group_created_date'] = pd.to_datetime(df_first['group_created_date'])
            
            return df_first
    
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            release_conn(conn)
@st.cache_data
def get_aggregated_data():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            query = f"""
            WITH incentivized_groups AS (
                SELECT g.*,
                       o.discount_rule_id
                FROM "groups" g
                JOIN groups_carts gc ON gc.user_id = g.created_by AND g.id = gc.group_id
                JOIN orders o ON gc.id = o.groups_carts_id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
                    AND o.discount_rule_id IS NOT NULL
            ),
            incentivized_group_members AS (
                SELECT gc.user_id,
                       gc.group_id,
                       gc.id AS group_cart_id,
                       o.id AS order_id,
                       ig.status AS group_status,
                       o.created_at,
                       gc.quantity,
                       o.discount,
                       o.discount_rule_id,
                       CASE
                           WHEN ig.created_by = gc.user_id THEN true
                           ELSE false
                       END AS is_admin,
                       ROW_NUMBER() OVER (PARTITION BY gc.user_id ORDER BY gc.created_at ASC) AS rn
                FROM incentivized_groups ig
                JOIN groups_carts gc ON gc.group_id = ig.id
                JOIN orders o ON gc.id = o.groups_carts_id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
            )
            SELECT igm.group_status,
                   igm.created_at::DATE,
                   COUNT(igm.order_id) AS total_order,
                   COUNT(igm.order_id) FILTER (WHERE igm.discount_rule_id IS NOT NULL) AS total_order_with_discount,
                   COUNT(DISTINCT igm.user_id) FILTER (WHERE igm.is_admin = true) AS unique_admins,
                   COUNT(DISTINCT igm.user_id) FILTER (WHERE igm.is_admin = true AND rn = 1) AS unique_first_time_admins,
                   COUNT(DISTINCT igm.user_id) FILTER (WHERE rn = 1) AS first_time_ordering_customers,
                   SUM(igm.discount) FILTER (WHERE igm.discount_rule_id IS NOT NULL) AS total_discounts,
                   SUM(igm.quantity) AS total_quantity,
                   SUM(igm.quantity) FILTER (WHERE rn = 1) AS first_time_customer_quantity
            FROM incentivized_group_members igm
            GROUP BY igm.group_status, igm.created_at::DATE;
            """
            query_2 = f"""
            WITH discount_usage AS (
                SELECT
                    gc.user_id,
                    COUNT(o.id) AS usage_count
                FROM
                    orders o
                JOIN
                    groups_carts gc ON gc.id = o.groups_carts_id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
                JOIN
                    "groups" g ON g.id = gc.group_id
                    AND g.status = 'COMPLETED'
                WHERE
                    o.discount_rule_id IS NOT NULL
                GROUP BY
                    gc.user_id
            )
            SELECT
                usage_count,
                COUNT(distinct user_id) AS number_of_users
            FROM
                discount_usage
            GROUP BY
                usage_count
            ORDER BY
                usage_count;

            """
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=colnames)
            
            cur.execute(query_2)
            data_2 = cur.fetchall()
            colnames_2 = [desc[0] for desc in cur.description]
            df_2 = pd.DataFrame(data_2, columns=colnames_2)
            
            return df,df_2
    finally:
        if conn:
            release_conn(conn)
@st.cache_data
def fetch_all_data():
    
    query_all = f"""
    SELECT 
            DATE_TRUNC('day', gc.created_at)::date AS date,
            pn.name AS product_name,
            gd.max_group_member,
            SUM(gc.quantity) AS total_quantity
        FROM products p
        JOIN group_deals gd ON gd.product_id = p.id
        JOIN groups g ON g.group_deals_id = gd.id AND g.status = 'COMPLETED'
        JOIN groups_carts gc ON gc.group_id = g.id
        JOIN orders o ON o.groups_carts_id = gc.id AND o.status = 'COMPLETED'
        JOIN product_names pn ON pn.id = p.name_id
        --WHERE g.created_at BETWEEN %s AND %s + interval '1 day' - interval '1 second'
        GROUP BY date, pn.name, gd.max_group_member
        ORDER BY date, total_quantity DESC;
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query_all)
            data_all = cur.fetchall()
            colnames_data_all = [desc[0] for desc in cur.description]
            df_ = pd.DataFrame(data_all, columns=colnames_data_all)
            return df_
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            release_conn(conn)
            
def daily_GLAC_data():
    query_GLAC = f"""
        WITH overall_orders AS (
        SELECT gc.user_id,
        gc.group_id,
        gc.id AS group_cart_id,
        o.id AS order_id,
        o.created_at,
        g.status AS group_status,
        gc.quantity,
        o.discount,
        o.discount_type,
        o.discount_rule_id,
        CASE
        WHEN g.created_by = gc.user_id THEN true
        ELSE false
        END AS is_admin,
        ROW_NUMBER() OVER (
        PARTITION BY gc.user_id
        ORDER BY o.created_at ASC
        ) AS rn
        FROM groups_carts gc
        JOIN groups g on g.id = gc.group_id
        JOIN orders o ON gc.id = o.groups_carts_id
        AND o.status = 'COMPLETED'
        AND o.deleted_at IS NULL
        ),
        admin_orders AS (
        SELECT gc.user_id,
        gc.group_id,
        gc.id AS group_cart_id,
        o.id AS order_id,
        o.created_at,
        CASE
        WHEN g.status = 'COMPLETED' THEN 1
        WHEN g.status = 'FAILED' THEN 2
        ELSE 3
        END AS priority,
        ROW_NUMBER() OVER (
        PARTITION BY gc.user_id
        ORDER BY CASE
        WHEN g.status = 'COMPLETED' THEN 1
        WHEN g.status = 'FAILED' THEN 2
        ELSE 3
        END,
        o.created_at ASC
        ) AS admin_number
        FROM groups g
        JOIN groups_carts gc ON g.id = gc.group_id
        JOIN orders o ON gc.id = o.groups_carts_id
        WHERE o.status = 'COMPLETED'
        AND o.deleted_at IS NULL
        AND g.created_by = gc.user_id
        ),
        group_members as (
        SELECT o.user_id,
        o.group_id,
        o.group_cart_id,
        o.order_id,
        o.created_at::DATE AS created_at,
        COALESCE(a.admin_number, NULL) AS admin_number,
        o.rn AS order_number,
        o.group_status,
        o.is_admin,
        o.discount_rule_id,
        o.rn,
        o.discount_type,
        o.discount
        FROM overall_orders o
        LEFT JOIN admin_orders a ON o.order_id = a.order_id
        ORDER BY o.user_id,
        o.rn ASC
        )
        SELECT created_at::DATE,
        group_status,
        COUNT(DISTINCT group_id) AS total_group,
        COUNT(DISTINCT user_id) FILTER (WHERE is_admin = true) AS total_unique_group_leaders,
        COUNT(DISTINCT user_id) FILTER (WHERE admin_number = 1) AS total_new_group_leaders,
        COUNT(DISTINCT user_id) FILTER (WHERE admin_number = 1 AND discount_rule_id IS NOT NULL) AS total_new_group_leaders_with_discount,
        COUNT(DISTINCT user_id) FILTER (WHERE admin_number = 1 AND rn > 1) AS total_group_leaders_with_first_admin_order,
        sum(discount) FILTER(where discount_type = 'FIXED') total_discounts,
        sum(discount) FILTER(where discount_type = 'FIXED'and admin_number = 1) total_discounts_for_new_admin,
        CASE
                WHEN COUNT(DISTINCT user_id) FILTER (
                    WHERE admin_number = 1
                    AND discount_rule_id IS NOT NULL
                ) > 0 THEN
                    SUM(discount) FILTER (
                        WHERE discount_type = 'FIXED'
                    ) / COUNT(DISTINCT user_id) FILTER (
                        WHERE admin_number = 1
                        AND discount_rule_id IS NOT NULL
                    )
                ELSE NULL
            END AS group_leader_acquisition_cost,
        sum(discount) FILTER(where discount_type = 'FIXED'and is_admin = false) as total_discounts_members,
        count(DISTINCT user_id) filter(where is_admin = false and discount_rule_id is not null) as members_that_received_discounts,
        count(DISTINCT group_id) filter(where is_admin = false and discount_rule_id is not null) as group_that_receieved_members_discounts
        FROM group_members
        GROUP BY created_at::DATE,
        group_status
        ORDER BY created_at::DATE DESC

    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query_GLAC)
            data_all = cur.fetchall()
            colnames_data_all = [desc[0] for desc in cur.description]
            df_GLAC = pd.DataFrame(data_all, columns=colnames_data_all)
            return df_GLAC
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            release_conn(conn)

# Fetch data with caching
@st.cache_data
def fetch_data(start_date, end_date, frequency, selected_products, selected_group_size):
    date_trunc = {
        'Daily': "DATE_TRUNC('day', gc.created_at)::date",
        'Weekly': "DATE_TRUNC('week', gc.created_at)::date",
        'Monthly': "DATE_TRUNC('month', gc.created_at)::date"
    }.get(frequency, "DATE_TRUNC('day', gc.created_at)::date")

    params = [start_date, end_date]
    product_filter = ""
    group_size_filter = ""

    if selected_products:
        product_placeholders = ', '.join(['%s'] * len(selected_products))
        product_filter = f"AND pn.name IN ({product_placeholders})"
        params.extend(selected_products)
    
    if selected_group_size:
        group_size_placeholders = ', '.join(['%s'] * len(selected_group_size))
        group_size_filter = f"AND gd.max_group_member IN ({group_size_placeholders})"
        params.extend(selected_group_size)

    query = f"""
        WITH first_order AS (
            SELECT
                gc.user_id,
                gc.group_id,
                gc.id AS group_cart_id,
                gc.created_at,
                ROW_NUMBER() OVER (PARTITION BY gc.user_id ORDER BY gc.created_at ASC) AS rn
            FROM
                groups_carts gc
            JOIN
                orders o ON gc.id = o.groups_carts_id
            WHERE
                o.status = 'COMPLETED'
                AND o.deleted_at IS NULL
        ),
        group_quantity AS (
            SELECT
                g.id,
                SUM(gc.quantity) AS total_quantity
            FROM
                groups g
            JOIN group_deals gd ON gd.id = g.group_deals_id
                AND g.status = 'COMPLETED'
                AND gd.product_id IS NOT NULL
            JOIN groups_carts gc ON gc.group_id = g.id
            JOIN orders o ON o.groups_carts_id = gc.id
                AND o.status = 'COMPLETED'
                AND o.deleted_at IS NULL
            WHERE
                g.created_at BETWEEN %s AND %s + interval '1 day' - interval '1 second'
            GROUP BY
                g.id
        ),
        first_time_customers AS (
            SELECT
                fo.group_id,
                COUNT(DISTINCT fo.user_id) AS first_time_customers
            FROM
                first_order fo
            WHERE
                fo.rn = 1
            GROUP BY
                fo.group_id
        )
        SELECT
            {date_trunc} AS date,
            pn.name AS product_name,
            gd.max_group_member,
            g.created_by,
            COALESCE(ftc.first_time_customers, 0) AS first_time_customers,
            COUNT(DISTINCT u.id) AS customer_created_after_group,
            COUNT(DISTINCT u2.id) AS customer_created_after_group_deal,
            COUNT(DISTINCT gc.id) AS num_of_group_carts,
            gq.total_quantity
        FROM
            products p
        JOIN group_deals gd ON gd.product_id = p.id
        JOIN groups g ON g.group_deals_id = gd.id
            AND g.status = 'COMPLETED'
        JOIN groups_carts gc ON gc.group_id = g.id
        JOIN orders o ON o.groups_carts_id = gc.id
            AND o.status = 'COMPLETED'
            AND o.deleted_at IS NULL
        JOIN product_names pn ON pn.id = p.name_id
        JOIN group_quantity gq ON gq.id = g.id
        LEFT JOIN first_time_customers ftc ON ftc.group_id = g.id
        LEFT JOIN users u ON u.id = gc.user_id
            AND u.created_at > g.created_at
        LEFT JOIN users u2 ON u2.id = gc.user_id
            AND u2.created_at > gd.created_at
        WHERE
            p.id IS NOT NULL
            {product_filter}
            {group_size_filter}
        GROUP BY
            date,
            g.id,
            g.created_by,
            pn.name,
            gq.total_quantity,
            ftc.first_time_customers,
            gd.max_group_member
        ORDER BY
            date, pn.name;
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            data = cur.fetchall()
            
            if not data:  # Check if no data is returned
                st.error("No data found for the selected date range.")
                return pd.DataFrame()
            
            colnames_data = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=colnames_data)

            # Ensure Arrow compatibility by converting to numeric types where necessary
            df['max_group_member'] = pd.to_numeric(df['max_group_member'], errors='coerce')
            df['first_time_customers'] = pd.to_numeric(df['first_time_customers'], errors='coerce')
            df['customer_created_after_group'] = pd.to_numeric(df['customer_created_after_group'], errors='coerce')
            df['customer_created_after_group_deal'] = pd.to_numeric(df['customer_created_after_group_deal'], errors='coerce')
            df['num_of_group_carts'] = pd.to_numeric(df['num_of_group_carts'], errors='coerce')
            df['total_quantity'] = pd.to_numeric(df['total_quantity'], errors='coerce')

            # Calculate the summary metrics
            total_num_of_group = df['num_of_group_carts'].count()

            # Calculate the sum for all numeric columns except 'max_group_member'
            totals = df.sum(numeric_only=True)

            # Preserve the original value of 'max_group_member'
            totals['max_group_member'] = df['max_group_member'].iloc[0]

            # Add a new 'Row Type' column to differentiate between data and summary rows
            df['Row Type'] = 'Data'

            # Prepare the total row
            total_row = pd.DataFrame([totals], columns=totals.index)
            total_row['Row Type'] = 'Total'
            
            # Add the CA row (handling division by zero)
            ca_row = pd.DataFrame([{
                'date': np.nan,
                'product_name': np.nan,
                'max_group_member': np.nan,
                'first_time_customers': (df['first_time_customers'].sum() / total_num_of_group
                                        if total_num_of_group > 0 else np.nan),
                'customer_created_after_group': (df['customer_created_after_group'].sum() / total_num_of_group
                                                if total_num_of_group > 0 else np.nan),
                'customer_created_after_group_deal': (df['customer_created_after_group_deal'].sum() / total_num_of_group
                                                    if total_num_of_group > 0 else np.nan),
                'num_of_group_carts': np.nan,
                'total_quantity': np.nan,
                'Row Type': 'CA'
            }])

            # Add the per Group Quantity row (handling division by zero)
            per_group_quantity_row = pd.DataFrame([{
                'date': np.nan, 'product_name': np.nan, 'max_group_member': np.nan,
                'first_time_customers': np.nan, 'customer_created_after_group': np.nan,
                'customer_created_after_group_deal': np.nan, 'num_of_group_carts': np.nan,
                'total_quantity': (df['total_quantity'].sum() / total_num_of_group
                                if total_num_of_group > 0 else np.nan),
                'Row Type': 'per Group Quantity'
            }])

            # Add the Engagement Ratio row (handling division by zero)
            s = (total_num_of_group or 0) * df['max_group_member'].max()
            engagement_ratio = df['total_quantity'].sum() / s if s > 0 else np.nan

            engagement_ratio_row = pd.DataFrame([{
                'date': np.nan, 'product_name': np.nan, 'max_group_member': np.nan,
                'first_time_customers': np.nan, 'customer_created_after_group': np.nan,
                'customer_created_after_group_deal': np.nan, 'num_of_group_carts': np.nan,
                'total_quantity': engagement_ratio,
                'Row Type': 'Engagement ratio'
            }])

            # Concatenate all rows
            df = pd.concat([df, total_row, ca_row, per_group_quantity_row, engagement_ratio_row], ignore_index=True)

            # Drop 'created_by' as it is not needed
            df = df.drop(columns=['created_by'], errors='ignore')

            return df
    finally:
        if conn:
            release_conn(conn)


def aggregate_by_week(data, metrics_info, start_date, end_date):
    if 'group_created_date' not in data.columns:
        raise ValueError("The input data must contain a 'group_created_date' column.")
    
    # Convert 'group_created_date' to datetime
    data.loc[:, 'group_created_date'] = pd.to_datetime(data['group_created_date'])
    
    # Filter data within the selected date range
    data = data[(data['group_created_date'] >= start_date) & (data['group_created_date'] <= end_date)]

    # Adjust to ensure the week starts on Wednesday and ends on Tuesday
    data.loc[:,'group_created_date'] -= pd.to_timedelta((data['group_created_date'].dt.weekday + 1) % 7, unit='d')
    
    # Set the index to group_created_date for resampling
    data = data.set_index('group_created_date')
    
    # Resample data to weekly, ending on Wednesday
    aggregated_data = data.resample('W-WED').agg(metrics_info).reset_index()

    # Create a full range of weeks within the selected date range
    start_date_aligned = start_date - pd.to_timedelta((start_date.weekday() + 1) % 7, unit='d')
    full_weeks = pd.date_range(start=start_date_aligned, end=end_date, freq='W-WED')
    
    full_data = pd.DataFrame({'group_created_date': full_weeks})
   
    # Merge full_data with aggregated_data
    aggregated_data = pd.merge(full_data, aggregated_data, on='group_created_date', how='left').fillna(0)
    
    # Ensure data is sorted by the actual date before creating week ranges
    aggregated_data = aggregated_data.sort_values('group_created_date')

    # Create week range labels
    aggregated_data['week_start'] = aggregated_data['group_created_date'].dt.strftime('%b %d')
    aggregated_data['week_end'] = (aggregated_data['group_created_date'] + pd.DateOffset(days=6)).dt.strftime('%b %d')
    aggregated_data['frequency'] = aggregated_data['week_start'] + ' to ' + aggregated_data['week_end'] 
    return aggregated_data


def aggregate_by_day(data, metrics_info, start_date, end_date):
    if 'group_created_date' not in data.columns:
        raise ValueError("The input data must contain a 'group_created_date' column.")
    data.loc[:, 'group_created_date'] = pd.to_datetime(data['group_created_date'])

    data = data[(data['group_created_date'] >= start_date) & (data['group_created_date'] <= end_date)]
    
    data = data.set_index('group_created_date')
    aggregated_data = data.resample('D').agg(metrics_info).reset_index()
    
    full_days = pd.date_range(start=start_date, end=end_date, freq='D')
    full_data = pd.DataFrame({'group_created_date': full_days})
    for metric in metrics_info.keys():
        full_data[f'{metric}_agg'] = 0
    aggregated_data = pd.merge(full_data, aggregated_data, on='group_created_date', how='left')
    aggregated_data = aggregated_data.infer_objects(copy=False).fillna(0)

    # Optional: Set the pandas option to opt-in to the future behavior
    pd.set_option('future.no_silent_downcasting', True)
    
    aggregated_data = aggregated_data.sort_values('group_created_date')
    aggregated_data['day'] = aggregated_data['group_created_date'].dt.strftime('%b %d')
    
    aggregated_data = aggregated_data.rename(columns={'day': 'frequency'})
    
    return aggregated_data


def aggregate_by_month(data, metrics_info,start_date, end_date):
    if 'group_created_date' not in data.columns:
        raise ValueError("The input data must contain a 'group_created_date' column.")
    data.loc[:, 'group_created_date'] = pd.to_datetime(data['group_created_date'])

    data = data[data['group_created_date'] >= '2023-10-01']
    
    data = data.set_index('group_created_date')
    
    aggregated_data = data.resample('M').agg(metrics_info).reset_index()
    
    full_months = pd.date_range(start=start_date, end=end_date, freq='M')
    full_data = pd.DataFrame({'group_created_date': full_months})
    for metric in metrics_info.keys():
        full_data[f'{metric}_agg'] = 0
    aggregated_data = pd.merge(full_data, aggregated_data, on='group_created_date', how='left').fillna(0)
    
    aggregated_data = aggregated_data.sort_values('group_created_date')
    aggregated_data['month'] = aggregated_data['group_created_date'].dt.strftime('%B')
    
    aggregated_data = aggregated_data.rename(columns={'month': 'frequency'})
    
    return aggregated_data

def compute_percentage_change(aggregated_data, metric):
  
    if metric not in aggregated_data.columns:
        raise KeyError(f"Metric '{metric}' not found in aggregated data columns.")
    
    aggregated_data['previous_week'] = aggregated_data[metric].shift(1)
    aggregated_data['previous_2_week'] = aggregated_data[metric].shift(2)

    aggregated_data['percentage_change'] = (aggregated_data['previous_week']- aggregated_data['previous_2_week'] ) / \
                                            aggregated_data['previous_2_week'].replace(0, np.nan) * 100
   
    aggregated_data['percentage_change'] = aggregated_data['percentage_change'].fillna(0)

    return aggregated_data

# Function to show metric trend
import plotly.graph_objs as go
def show_metric_trend_weekly(data, selected_frequency, start_date, end_date):
    single_chart_metrics = ['average_completion_duration', 'unique_group_leaders', 'new_group_leaders']
    dual_chart_metrics = [
        ['completed_groups', 'failed_groups'], 
        ['success_rate', 'failure_rate'], 
        ['unique_group_leaders_success_rate', 'unique_group_leaders_failure_rate'], 
        ['new_group_leaders_success_rate', 'new_group_leaders_failure_rate']
    ]
    combined_chart_metrics = [['average_group_size', 'number_of_orders']]

    color_map = {
        'completed_groups': 'green',
        'failed_groups': 'red',
        'success_rate': 'green',
        'failure_rate': 'red',
        'unique_group_leaders_success_rate': 'green',
        'unique_group_leaders_failure_rate': 'red',
        'new_group_leaders_success_rate': 'green',
        'new_group_leaders_failure_rate': 'red',
        'average_group_size': 'green',
        'number_of_orders': 'red',
    }
    metrics_info = {
        'completed_groups': 'sum',
        'failed_groups': 'sum',
        'average_completion_duration': 'mean',
        'unique_group_leaders':'sum', 
        'new_group_leaders':'sum',
        'success_rate': 'mean',
        'failure_rate': 'mean',
        'unique_group_leaders_success_rate': 'mean',
        'unique_group_leaders_failure_rate': 'mean',
        'new_group_leaders_success_rate': 'mean',
        'new_group_leaders_failure_rate': 'mean',
        'average_group_size': 'mean',
        'number_of_orders': 'mean'
    }
    
    if data.empty:
        st.markdown("<div style='background-color: #f8d7da; padding: 20px; border-radius: 5px;'><h3>No data available for the selected date range.</h3></div>", unsafe_allow_html=True)
        return

    for metric in single_chart_metrics:
        metrics_info_local = {metric: metrics_info[metric]}
        if selected_frequency == 'weekly':
            aggregated_data = aggregate_by_week(data, metrics_info_local,start_date, end_date)
        elif selected_frequency == 'monthly':
            aggregated_data = aggregate_by_month(data, metrics_info_local,start_date, end_date)
        else:
            aggregated_data = aggregate_by_day(data, metrics_info_local, start_date, end_date)
                
        aggregated_data = compute_percentage_change(aggregated_data, metric)
      
        if len(aggregated_data) < 2:
            st.markdown(f"""
            <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                <h3>{metric.replace("_", " ").title()}</h3>
                <p>Not enough data to display trend</p>
            </div>
            """, unsafe_allow_html=True)
            continue

        last_week = aggregated_data.iloc[-1]
        previous_week = aggregated_data.iloc[-2]
        percentage_change_last_week = last_week['percentage_change']

        st.markdown(f"""
        <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
            <h3>{metric.replace("_", " ").title()}</h3>
            <p>From {previous_week['frequency']} to {last_week['frequency']}</p>
            <p>Percentage Change: {percentage_change_last_week:.2f}%</p>
        </div>
        """, unsafe_allow_html=True)

        fig = px.line(aggregated_data, x='frequency', y=metric,
                      title=f'{metric.replace("_", " ").title()} {selected_frequency.title()} Trend')
        fig.update_traces(line=dict(color=color_map.get(metric, 'blue')))
        fig.update_layout(
            autosize=False,
            width=1600,
            height=800,
            margin=dict(l=40, r=40, b=200, t=40)
        )
        fig.update_xaxes(
            tickangle=300,
            tickformat='%d %b',
            tickmode='linear',
            dtick=1,
            title=selected_frequency,
            automargin=True
        )
        fig.update_yaxes(title=f"{metric.replace('_', ' ').title()}")
        fig.update_traces(mode='lines+markers')
        st.plotly_chart(fig)
    
    for metrics_pair in dual_chart_metrics:
        metric1, metric2 = metrics_pair
        metrics_info_local = {metric1: metrics_info[metric1], metric2: metrics_info[metric2]}
        if selected_frequency == 'weekly':
            aggregated_data = aggregate_by_week(data, metrics_info_local,start_date, end_date)
        elif selected_frequency == 'monthly':
            aggregated_data = aggregate_by_month(data, metrics_info_local,start_date, end_date)
        else:
            aggregated_data = aggregate_by_day(data, metrics_info_local, start_date, end_date)
        
        metric1_column = f'{metric1}'
        metric2_column = f'{metric2}'
        
        aggregated_data1 = compute_percentage_change(aggregated_data, metric1_column)
        aggregated_data2 = compute_percentage_change(aggregated_data, metric2_column)
        
        # st.write(aggregated_data)
        
        if len(aggregated_data) < 2:
            st.markdown(f"""
            <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()}</h3>
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
            <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()}</h3>
            <p>From {previous_week['frequency']} to {last_week['frequency']}</p>
            <p>{metric1.replace("_", " ").title()} Percentage Change: {percentage_change_last_week_metric1:.2f}%</p>
            <p>{metric2.replace("_", " ").title()} Percentage Change: {percentage_change_last_week_metric2:.2f}%</p>
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
            height=800,
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
            title=selected_frequency,
            automargin=True
        )
        fig.update_yaxes(title=f"{metric1.replace('_', ' ').title()} and {metric2.replace('_', ' ').title()}")
        st.plotly_chart(fig)

    
    for metrics_pair in combined_chart_metrics:
        metric1, metric2 = metrics_pair
        metrics_info_local = {metric1: metrics_info[metric1], metric2: metrics_info[metric2]}
        if selected_frequency == 'weekly':
            aggregated_data = aggregate_by_week(data, metrics_info_local,start_date, end_date)
        elif selected_frequency == 'monthly':
            aggregated_data = aggregate_by_month(data, metrics_info_local,start_date, end_date)
        else:
            aggregated_data = aggregate_by_day(data, metrics_info_local, start_date, end_date)
        
        metric1_column = f'{metric1}'
        metric2_column = f'{metric2}'
        
        aggregated_data1 = compute_percentage_change(aggregated_data, metric1_column)
        aggregated_data2 = compute_percentage_change(aggregated_data, metric2_column)
        
        if len(aggregated_data) < 2:
            st.markdown(f"""
            <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
                <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()}</h3>
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
            <h3>{metric1.replace("_", " ").title()} and {metric2.replace("_", " ").title()}</h3>
            <p>From {previous_week['frequency']} to {last_week['frequency']}</p>
            <p>{metric1.replace("_", " ").title()} Percentage Change: {percentage_change_last_week_metric1:.2f}%</p>
            <p>{metric2.replace("_", " ").title()} Percentage Change: {percentage_change_last_week_metric2:.2f}%</p>
        </div>
        """, unsafe_allow_html=True)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=aggregated_data2['frequency'], 
                                y=aggregated_data2[metric2_column],
                                mode='lines+markers',
                                name=metric2.replace("_", " ").title(), 
                                line=dict(color=color_map.get(metric2, 'blue'))))
        fig.add_trace(go.Bar(
            x=aggregated_data1['frequency'], 
            y=aggregated_data1[metric1_column],
            name=metric1.replace("_", " ").title(), 
            marker_color=color_map.get(metric1, 'red')
        ))

        fig.update_layout(
            autosize=False,
            width=1600,
            height=800,
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
            title=selected_frequency,
            automargin=True
        )
        fig.update_yaxes(title=f"{metric1.replace('_', ' ').title()} and {metric2.replace('_', ' ').title()}")
        st.plotly_chart(fig)

def calculate_kpis(df):
    total_group_leader = df['group_leader'].count()
    total_unique_group_leader = df['group_leader'].nunique()
    total_New_group_leader = df[df['is_new_group_leader'] == 1]['group_leader'].nunique()
    return pd.Series({
        'Total Group Leader': total_group_leader,
        'Total Unique Group Leader': total_unique_group_leader,
        'Total New Group Leaders':total_New_group_leader
    })


def resample_data(df, freq):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Expected a DataFrame, got something else.")
    if 'group_created_date' not in df.columns:
        raise KeyError("'group_created_date' column not found in DataFrame")
    
    # Ensure the 'group_created_date' column is datetime type
    df.loc[:, 'group_created_date'] = pd.to_datetime(df['group_created_date'])
    
    # Check if the temporary DataFrame is empty
    if df.empty:
        return pd.DataFrame(columns=['group_created_date', 'Total Group Leader', 'Total Unique Group Leader', 'Total New Group Leaders'])
    
    
    if freq == 'daily':
        return df.resample('D', on='group_created_date').apply(calculate_kpis).reset_index()
    elif freq == 'weekly':
        return df.resample('W-Mon', on='group_created_date').apply(calculate_kpis).reset_index()
    elif freq == 'monthly':
        return df.resample('M', on='group_created_date').apply(calculate_kpis).reset_index()

# Streamlit application
st.title('Group Metrics Trend Dashboard')

# Date range filter
default_start_date = datetime.today() - timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Frequency filter
frequency_options = ['daily', 'weekly', 'monthly']
selected_frequency = st.sidebar.selectbox('Select Frequency', frequency_options)
# Fetch aggregated data
data_first = fetch_aggregated_data(start_date,end_date)
data_incentivized, data_usage = get_aggregated_data()

# Adding status filter
status_options = ['all', 'formed', 'COMPLETED', 'FAILED']
selected_status = st.sidebar.selectbox('Select Status', status_options)

tab1, tab2, tab3, tab4 = st.tabs(["Group weekly report","Group Size and Order Analysis", "Incentivized Groups", "Daily GLAC Report"])

# Display metrics and trends
if st.sidebar.button("Filter"):
    with tab1:
        # if start_date == end_date:
        #     end_date += pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
      
        # data_filtered = data_first[(data_first['group_created_date'].between(start_date, end_date))]
        
        if selected_status == 'all':
            # data_filtered
            data_first = data_first[(data_first['group_created_date'] >= start_date) & (data_first['group_created_date'] <= end_date)]
        elif selected_status == 'formed': 
        # Include rows where 'status' is NaN or not in ['COMPLETED', 'FAILED']
            data_first = data_first[data_first['status'].isin(['ACTIVE', 'PENDING'])]
        elif selected_status == 'COMPLETED':
            # Filter rows where 'status' is 'COMPLETED'
            data_first = data_first[data_first['status'] == 'COMPLETED']
        elif selected_status == 'FAILED':
            # Filter rows where 'status' is 'FAILED'
            data_first = data_first[data_first['status'] == 'FAILED']
      
        kpis_filtered = resample_data(data_first, selected_frequency)
        
        st.dataframe(kpis_filtered.set_index('group_created_date'),height=400, width=800)

        # Plotting line charts for the KPIs
        st.line_chart(kpis_filtered.set_index('group_created_date')[[ 'Total Group Leader', 'Total Unique Group Leader','Total New Group Leaders']])
        with st.expander("Group Metrics Trends"):
            show_metric_trend_weekly(data_first,selected_frequency,start_date,end_date)
with tab2:
    # Convert dates to datetime format and normalize
    start_date = pd.to_datetime(start_date).normalize()
    end_date = pd.to_datetime(end_date).normalize()

    # Fetch all data without group size filters
    df_1 = fetch_all_data()
    if not df_1.empty:
        # Filter and display group sizes based on actual data in df_1
        st.header("Explore Customer Acquisition by Selecting Products and Group Sizes")
        most_sold_products = df_1.groupby('product_name')['total_quantity'].sum().reset_index().sort_values(by='total_quantity', ascending=False)
        sorted_product_names = most_sold_products['product_name'].tolist()
        selected_products = st.multiselect("Select products", sorted_product_names)

        # Filter df_1 by selected products and display the unique group sizes
        filtered_df = df_1[df_1['product_name'].isin(selected_products)] if selected_products else df_1
        group_size = filtered_df['max_group_member'].unique().tolist()
        selected_group_size = st.multiselect("Select group size", group_size)

      # Add the new metrics
    df = fetch_data(start_date, end_date, selected_frequency, selected_products, selected_group_size)
    
    selected_products_str = ', '.join(selected_products) if selected_products else "All Products"
    selected_group_size_str = ', '.join(map(str, selected_group_size)) if selected_group_size else "All Group Sizes"
    
    st.subheader(
        f"Customer Acquisition and Engagement Ratio Table for {selected_products_str} with group size{selected_group_size_str} "
        f"from {start_date.date()} to {end_date.date()} with {selected_frequency} frequency"
    )

    st.write(df)

    # Add traces for each top product
    def plot_time_series_with_metrics(df, metric_name):
    
        # Get top products with at least one non-zero value for the specified metric
        top_products = df[df[metric_name] > 0]['product_name'].value_counts().index[:5]
        
        fig = go.Figure()

        for product in top_products:
            filtered_df = df[df['product_name'] == product]
            # Aggregate total quantity per date for the product
            product_total_quantity = filtered_df.groupby('date')['total_quantity'].sum().reset_index()
            
            if not filtered_df[metric_name].sum() > 0:
                continue
            
            hover_text = filtered_df.apply(
                lambda row: f"Product: {row['product_name']}<br>Quantity: {row['total_quantity']}<br>Date: {row['date']}<br>{metric_name}:{row[metric_name]}", axis=1
            )
            
            fig.add_trace(go.Bar(
                x=filtered_df['date'], 
                y=filtered_df[metric_name], 
                # mode='lines+markers', 
                name=f'{product} (quantity: {product_total_quantity["total_quantity"].sum()})',
                hovertext=hover_text,
                hoverinfo="text"
            ))

        fig.update_layout(
            title=f'Time Series Analysis of {metric_name.capitalize()} for products with high quantity',
            xaxis_title='Date',
            yaxis_title=metric_name.capitalize(),
            hovermode='closest',
            xaxis=dict(tickangle=-45) 
        )
        fig.update_xaxes(tickmode='linear', dtick='D1')  # Force display of every day
        
        return fig
    
    if df is not None and not df.empty:
        
        st.subheader("Correlation Heatmap")
        with st.expander("Expand this to get better understanding on the visualization"):
            st.write("""
            **Purpose**: The heatmap provides a visual representation of how strongly each pair of metrics is related to one another.
            
            **Explanation**: The heatmap uses colors to show the strength of the relationships between metrics like `max_group_member`,  `first_time_orders`,`customer_created_after_group`, `customer_created_after_group_deal` and `total_quantity`. A value close to 1 means the metrics are strongly positively correlated (they increase or decrease together), while a value close to -1 means they are negatively correlated (one increases as the other decreases). Values near 0 indicate little or no correlation.
            
            **Insights**: The heatmap helps identify which metrics move together. For example, if `max_group_member` is highly correlated with `first_time_orders`, it suggests that larger groups are likely to bring in more new customers. This insight can guide strategies, such as focusing on increasing group sizes to boost first-time orders.
            """)
        
        corr_matrix = df[['max_group_member', 'first_time_customers','customer_created_after_group','customer_created_after_group_deal',  'total_quantity']].corr()
        plt.figure(figsize=(10, 8))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
        plt.title('Correlation Heatmap')
        st.pyplot(plt)
        
        st.subheader("Time Series Analysis")
        with st.expander("Expand this to get better understanding on the visualization"):
            st.write("""
            **Purpose**: This line chart shows how each metric (e.g.,  `first_time_customers`) changes over time.
            
            **Explanation**: Each line represents a different metric, with time (date) on the x-axis and the metric values on the y-axis. By overlaying these lines, we can observe how these metrics fluctuate together over time.
            
            **Insights**: This visualization helps identify patterns and trends. For example, you might see that certain date have higher  values, which correspond to peaks in `first_time_customers`. This can help in understanding seasonal trends or the impact of specific campaigns on group size and customer behavior.
            """)
       
        aggregated_df = df.groupby('date')['first_time_customers'].sum().reset_index()
        
        # Reindex the aggregated DataFrame to include all dates
        aggregated_df = aggregated_df.set_index('date').fillna(0).reset_index()
        aggregated_df.columns = ['date', 'first_time_customers']  # Rename columns

        # Create a bar chart for first-time customers over time
        fig5 = go.Figure()

        # Add bar trace
        fig5.add_trace(go.Bar(x=aggregated_df['date'], y=aggregated_df['first_time_customers'], name='First-Time Customers'))

        # Update layout
        fig5.update_layout(
            title='Time Series Analysis of First-Time Customers',
            xaxis_title='Date',
            yaxis_title='First-Time Customers',
            barmode='group',  # Optional: group bars together if you have multiple categories
            xaxis=dict(tickangle=-45)  # Optional: adjust x-axis tick labels
        )
        # Ensure all dates are displayed
        fig5.update_xaxes(tickmode='linear', dtick='D1')  # Force display of every day


        # Render the chart in Streamlit
        st.plotly_chart(fig5)
      
        fig4_new = go.Figure()
        # Filter out products with no first-time order values
        df = df[df['first_time_customers'] > 0]
        
        # Grouping by product names to calculate the total quantity for each product
        product_quantities = df.groupby('product_name')['total_quantity'].sum().reset_index()
        
        
        # Merging the total quantities back with the original DataFrame
        df = df.merge(product_quantities, on='product_name', suffixes=('', '_total'))
        
        
        fig = plot_time_series_with_metrics(df, 'first_time_customers')
        st.plotly_chart(fig)
        
with tab3:
    if start_date == end_date:
            end_date += pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    metrics = {
    "Total Orders": data_incentivized["total_order"].sum(),
    "Orders with Discount": data_incentivized["total_order_with_discount"].sum(),
    "Unique First-Time Admins": data_incentivized["unique_first_time_admins"].sum(),
    "First-Time Ordering Customers": data_incentivized["first_time_ordering_customers"].sum(),
    "Total Discounts Given": f"{data_incentivized['total_discounts'].sum():,.2f}Birr",
    }

    st.subheader("Incentivized Groups Summary Metrics")

    # Arrange metrics in 2 rows of 4 columns each
    rows = [st.columns(4), st.columns(4)]
    keys = list(metrics.keys())

    # Assign each metric to a card in the grid
    for i, (key, value) in enumerate(metrics.items()):
        row = rows[i // 4]
        with row[i % 4]:
            st.metric(label=key, value=value)
    st.subheader("Discount Utilization Patterns by User Frequency")
    st.write("""
    The KPI tracks:
    1. **usage_count**: The frequency with which users utilized discounts in completed orders.
    2. **number_of_users**: The number of unique users associated with each level of discount usage frequency.
    
    """)
 # Drop index directly and reset
    data_usage = data_usage.reset_index(drop=True, inplace=False)

    st.write(data_usage)
with tab4:
    if start_date > end_date:
        st.error("Start Date cannot be after End Date.")
    else:
        # Fetch and filter data
        df_GLAC = daily_GLAC_data()
        if not df_GLAC.empty:
            # Convert the created_at column to datetime
            df_GLAC['created_at'] = pd.to_datetime(df_GLAC['created_at'])
            # Apply date filter
            df_filtered = df_GLAC[(df_GLAC['created_at'] >= pd.Timestamp(start_date)) & 
                                (df_GLAC['created_at'] <= pd.Timestamp(end_date))]

            # Apply aggregation based on the frequency
            if selected_frequency == "weekly":
                df_filtered['period'] = df_filtered['created_at'].dt.to_period('W').apply(lambda r: r.start_time)
            elif selected_frequency == "monthly":
                df_filtered['period'] = df_filtered['created_at'].dt.to_period('M').apply(lambda r: r.start_time)
            else:
                df_filtered['period'] = df_filtered['created_at']

            # Aggregate data
            aggregated_data = df_filtered.groupby(['period', 'group_status']).agg({
                'total_group': 'sum',
                'total_unique_group_leaders': 'sum',
                'total_new_group_leaders_with_discount': 'sum',
                'total_new_group_leaders': 'sum',
                'total_discounts': 'sum',
                'total_discounts_for_new_admin': 'sum',
                'group_leader_acquisition_cost': 'mean', 
                'total_discounts_members':'sum',
                'members_that_received_discounts': 'sum',
                'group_that_receieved_members_discounts': 'sum'
                
            }).reset_index()

            # Display data
            st.subheader("Daily GLAC Data")
            st.dataframe(aggregated_data)

       # Filter data for group status = 'COMPLETED'
            completed_data = aggregated_data[aggregated_data['group_status'] == 'COMPLETED']

            # Plotly Visualization
            st.subheader("Group Leaders Summary (COMPLETED)")

            fig = go.Figure()

            # Add lines with marker values for each data series
            fig.add_trace(go.Scatter(
                x=completed_data['period'],
                y=completed_data['total_unique_group_leaders'],
                mode='lines+markers+text',
                name='Total Unique Group Leaders',
                line=dict(color='blue'),
                text=completed_data['total_unique_group_leaders'],  # Add values
                textposition='top center'  # Position of values
            ))

            fig.add_trace(go.Scatter(
                x=completed_data['period'],
                y=completed_data['total_new_group_leaders'],
                mode='lines+markers+text',
                name='Total New Group Leaders',
                line=dict(color='red'),
                text=completed_data['total_new_group_leaders'],  # Add values
                textposition='top center'
            ))

            fig.add_trace(go.Scatter(
                x=completed_data['period'],
                y=completed_data['total_new_group_leaders_with_discount'],
                mode='lines+markers+text',
                name='Total New Group Leaders with Discount',
                line=dict(color='orange'),
                text=completed_data['total_new_group_leaders_with_discount'],  # Add values
                textposition='top center'
            ))

            # Customize layout
            fig.update_layout(
                title="Group Leaders Summary (COMPLETED)",
                xaxis_title="Period",
                yaxis_title="Count",
                legend_title="Metrics",
                template="plotly_white",
                xaxis=dict(
                    tickangle=-90  # Rotate the x-axis labels by -45 degrees
                )
            )

            # Show the chart
            st.plotly_chart(fig)
            # st.subheader("Visualization")
            # st.line_chart(aggregated_data.set_index('period')[['group_leader_acquisition_cost', 'total_new_group_leaders_with_discount']])
        else:
            st.warning("No data available for the selected period.")
    