import pandas as pd
import streamlit as st
import plotly.express as px
import datetime
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
from db_pool import get_conn, release_conn
import numpy as np
from st_aggrid import AgGrid, GridOptionsBuilder

st.title("Group Failure KPI Dashboard")

# Query and calculate metrics
def get_kpi_data(start_date,end_date):
    conn = get_conn()
    
    try:
        with conn.cursor() as cur:
            params = [start_date, end_date]       
            query_failed_group=f"""WITH failed_group_details AS (
                SELECT 
                    g.id AS group_id,
                    COUNT(gc.user_id) AS group_size_at_failure,
                    gd.max_group_member AS required_group_size,
                    g.created_at AS group_created_at,
                    g.updated_at AS group_failed_at
                FROM groups g
                JOIN groups_carts gc ON g.id = gc.group_id
                JOIN group_deals gd ON g.group_deals_id = gd.id
                WHERE g.status = 'FAILED' 
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY g.id, gd.max_group_member, g.created_at, g.updated_at
            )
            SELECT 
                COUNT(*) AS failed_groups,
                -- Count of failed groups with a size of 5 or more
                COUNT(CASE WHEN fg.group_size_at_failure >= 5 THEN 1 END) AS failed_groups_5_or_more,

                -- Percentage of failed groups with 5 or more members
                CASE 
                    WHEN COUNT(fg.group_id) > 0
                    THEN (COUNT(CASE WHEN fg.group_size_at_failure >= 5 THEN 1 END) * 100.0 
                        / COUNT(fg.group_id))
                    ELSE 0.0
                END AS percentage_failed_groups_5_or_more,
                AVG(group_size_at_failure) AS avg_group_size_at_failure,
                AVG(required_group_size) AS avg_required_group_size
                --MIN(group_size_at_failure) AS min_group_size_at_failure,
                --MAX(group_size_at_failure) AS max_group_size_at_failure
            FROM failed_group_details fg;
            """
           
            # Failed Unique Group Members:
            query_failed_unique_group_memeber = f"""
            SELECT COUNT(DISTINCT user_id) AS failed_unique_group_members
            FROM groups_carts gc
            JOIN groups g ON gc.group_id = g.id
            WHERE g.status = 'FAILED' AND gc.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second';
            """
            
            query_returned_leaders_as_members = f"""WITH leader_failures AS (
                    SELECT 
                        g.created_by AS leader_id, 
                        g.created_at AS group_created_at
                    FROM groups g
                    WHERE g.status = 'FAILED'
                    AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second' -- Date filter in CTE
                )
                SELECT 
                    COUNT(DISTINCT gc.user_id) AS returned_leaders_as_members,
                    CASE
                        WHEN AGE(g.created_at, lf.group_created_at) <= INTERVAL '24 hours' THEN '1 Day'
                        WHEN AGE(g.created_at, lf.group_created_at) > INTERVAL '24 hours' 
                            AND AGE(g.created_at, lf.group_created_at) <= INTERVAL '48 hours' THEN '2 Days'
                        WHEN AGE(g.created_at, lf.group_created_at) > INTERVAL '48 hours' 
                            AND AGE(g.created_at, lf.group_created_at) <= INTERVAL '72 hours' THEN '3 Days'
                        WHEN AGE(g.created_at, lf.group_created_at) > INTERVAL '72 hours' 
                            AND AGE(g.created_at, lf.group_created_at) <= INTERVAL '96 hours' THEN '4 Days'
                        WHEN AGE(g.created_at, lf.group_created_at) > INTERVAL '96 hours' 
                            AND AGE(g.created_at, lf.group_created_at) <= INTERVAL '120 hours' THEN '5 Days'
                        WHEN AGE(g.created_at, lf.group_created_at) > INTERVAL '120 hours' 
                            AND AGE(g.created_at, lf.group_created_at) <= INTERVAL '144 hours' THEN '6 Days'
                        ELSE 'More than 6 Days'
                    END AS time_interval
                FROM groups_carts gc
                JOIN groups g ON gc.group_id = g.id
                JOIN leader_failures lf ON gc.user_id = lf.leader_id
                WHERE 
                    g.created_at > lf.group_created_at
                    AND g.status = 'FAILED'
                    AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY time_interval;
                """
            query_returned_members_as_leaders = f"""WITH member_failures AS (
                SELECT 
                    gc.user_id AS member_id, 
                    g.created_at AS group_created_at
                FROM groups_carts gc
                JOIN groups g ON gc.group_id = g.id
                WHERE g.status = 'FAILED'
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            )
            SELECT 
                COUNT(DISTINCT g.created_by) AS returned_members_as_leaders,
                CASE
                    WHEN AGE(g.created_at, mf.group_created_at) <= INTERVAL '24 hours' THEN '1 Day'
                    WHEN AGE(g.created_at, mf.group_created_at) > INTERVAL '24 hours' 
                        AND AGE(g.created_at, mf.group_created_at) <= INTERVAL '48 hours' THEN '2 Days'
                    WHEN AGE(g.created_at, mf.group_created_at) > INTERVAL '48 hours' 
                        AND AGE(g.created_at, mf.group_created_at) <= INTERVAL '72 hours' THEN '3 Days'
                    WHEN AGE(g.created_at, mf.group_created_at) > INTERVAL '72 hours' 
                        AND AGE(g.created_at, mf.group_created_at) <= INTERVAL '96 hours' THEN '4 Days'
                    WHEN AGE(g.created_at, mf.group_created_at) > INTERVAL '96 hours' 
                        AND AGE(g.created_at, mf.group_created_at) <= INTERVAL '120 hours' THEN '5 Days'
                    WHEN AGE(g.created_at, mf.group_created_at) > INTERVAL '120 hours' 
                        AND AGE(g.created_at, mf.group_created_at) <= INTERVAL '144 hours' THEN '6 Days'
                    ELSE 'More than 6 Days'
                END AS time_interval
            FROM groups g
            JOIN member_failures mf ON g.created_by = mf.member_id
            WHERE 
                g.created_at > mf.group_created_at 
                AND g.status = 'FAILED'
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY time_interval;
            """
            
            query_returned_leaders_again = f""" WITH leader_failures AS (
                    SELECT created_by AS leader_id, created_at
                    FROM groups
                    WHERE status = 'FAILED'
                    AND created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                )
                SELECT 
                    COUNT(DISTINCT g.created_by) AS returned_leaders_again,
                    CASE
                        WHEN AGE(g.created_at, lf.created_at) <= INTERVAL '24 hours' THEN '1 Day'
                        WHEN AGE(g.created_at, lf.created_at) > INTERVAL '24 hours' 
                            AND AGE(g.created_at, lf.created_at) <= INTERVAL '48 hours' THEN '2 Days'
                        WHEN AGE(g.created_at, lf.created_at) > INTERVAL '48 hours' 
                            AND AGE(g.created_at, lf.created_at) <= INTERVAL '72 hours' THEN '3 Days'
                        WHEN AGE(g.created_at, lf.created_at) > INTERVAL '72 hours' 
                            AND AGE(g.created_at, lf.created_at) <= INTERVAL '96 hours' THEN '4 Days'
                        WHEN AGE(g.created_at, lf.created_at) > INTERVAL '96 hours' 
                            AND AGE(g.created_at, lf.created_at) <= INTERVAL '120 hours' THEN '5 Days'
                        WHEN AGE(g.created_at, lf.created_at) > INTERVAL '120 hours' 
                            AND AGE(g.created_at, lf.created_at) <= INTERVAL '144 hours' THEN '6 Days'
                        ELSE 'More than 6 Days'
                    END AS time_interval
                FROM groups g
                JOIN leader_failures lf ON g.created_by = lf.leader_id
                WHERE g.created_at > lf.created_at
                AND g.status = 'FAILED'
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY time_interval;"""
            
            # Query for Returned Members as Members:
            
            query_returned_members_as_members = f"""
                WITH member_failures AS (
                    SELECT 
                        gc.user_id AS member_id, 
                        g.created_at AS group_failed_at
                    FROM groups_carts gc
                    JOIN groups g ON gc.group_id = g.id
                    WHERE g.status = 'FAILED'
                    AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                )
                SELECT 
                    COUNT(DISTINCT gc.user_id) AS returned_members_as_members,
                    CASE
                        WHEN AGE(g.created_at, mf.group_failed_at) <= INTERVAL '24 hours' THEN '1 Day'
                        WHEN AGE(g.created_at, mf.group_failed_at) > INTERVAL '24 hours' 
                            AND AGE(g.created_at, mf.group_failed_at) <= INTERVAL '48 hours' THEN '2 Days'
                        WHEN AGE(g.created_at, mf.group_failed_at) > INTERVAL '48 hours' 
                            AND AGE(g.created_at, mf.group_failed_at) <= INTERVAL '72 hours' THEN '3 Days'
                        WHEN AGE(g.created_at, mf.group_failed_at) > INTERVAL '72 hours' 
                            AND AGE(g.created_at, mf.group_failed_at) <= INTERVAL '96 hours' THEN '4 Days'
                        WHEN AGE(g.created_at, mf.group_failed_at) > INTERVAL '96 hours' 
                            AND AGE(g.created_at, mf.group_failed_at) <= INTERVAL '120 hours' THEN '5 Days'
                        WHEN AGE(g.created_at, mf.group_failed_at) > INTERVAL '120 hours' 
                            AND AGE(g.created_at, mf.group_failed_at) <= INTERVAL '144 hours' THEN '6 Days'
                        ELSE 'More than 6 Days'
                    END AS time_interval
                FROM groups_carts gc
                JOIN groups g ON gc.group_id = g.id
                JOIN member_failures mf ON gc.user_id = mf.member_id
                WHERE 
                    g.created_at > mf.group_failed_at -- Returned after failing in a previous group
                    AND g.status != 'FAILED' -- The group they returned to should not have failed
                    AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY time_interval
                ORDER BY time_interval;
            """
            
            query_failed_unique_group_leader=f"""
            WITH leader_failures AS (
                SELECT 
                    g.created_by AS leader_id, 
                    COUNT(DISTINCT prev_groups.id) AS previous_group_count
                FROM groups g
                LEFT JOIN groups prev_groups ON g.created_by = prev_groups.created_by 
                    AND prev_groups.created_at < g.created_at
                WHERE g.status = 'FAILED'
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY g.created_by
            )
            SELECT 
                COUNT(DISTINCT leader_id) AS failed_unique_group_leaders,
                SUM(CASE 
                        WHEN lf.previous_group_count = 0 THEN 1 
                        ELSE 0 
                    END) AS new_leaders,
                SUM(CASE 
                        WHEN lf.previous_group_count > 0 THEN 1 
                        ELSE 0 
                    END) AS recurrent_leaders
            FROM leader_failures lf;
            """
            query_returned_leaders_failed_size=f"""WITH leader_failures AS (
                SELECT 
                    g.created_by AS leader_id, 
                    g.created_at AS group_created_at, 
                    gd.max_group_member,   -- Maximum group size
                    COUNT(gc.user_id) AS group_size_at_failure  -- Current group size at failure
                FROM groups g
                JOIN group_deals gd ON g.group_deals_id = gd.id
                LEFT JOIN groups_carts gc ON g.id = gc.group_id
                WHERE g.status = 'FAILED'
                AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
                GROUP BY g.id, gd.max_group_member
            )
            SELECT 
                COUNT(DISTINCT g.created_by) AS returned_leaders_again,
                lf.max_group_member,   -- Maximum group size for the group that failed
                lf.group_size_at_failure  -- Size at which the group failed
            FROM groups g
            JOIN leader_failures lf ON g.created_by = lf.leader_id
            WHERE g.created_at > lf.group_created_at
            AND g.status = 'FAILED'
            AND g.created_at BETWEEN %s AND %s + INTERVAL '1 day' - INTERVAL '1 second'
            GROUP BY lf.max_group_member, lf.group_size_at_failure;

            """
            
            query_failure_product = f"""
            with failed_group_details As(
            SELECT 
                    g.id AS group_id,
                    COUNT(distinct o.id) AS group_size_at_failure,  -- Group size at failure (count of users in the group)
                    gd.max_group_member AS required_group_size,  -- Required group size for the deal
                    gd.product_id AS product_id,  -- Product ID for the group deal
                    g.status AS group_status  -- Group status (FAILED or not)
                FROM
                    groups g
                
                JOIN
                    groups_carts gc ON g.id = gc.group_id  -- Join groups with carts to get quantity (group size at failure)
                JOIN 
                    orders o on o.groups_carts_id = gc.id
                JOIN
                    group_deals gd ON g.group_deals_id = gd.id  -- Join groups with group deals to get product and group size info
                WHERE
                    DATE(g.created_at) BETWEEN %s AND %s + interval '1 day' - interval '1 second'
                    AND gc.status ='COMPLETED'
                    AND o.status ='COMPLETED'
                    AND o.deleted_at is null
                    AND gc.deleted_at is null
                GROUP BY
                    g.id, gd.max_group_member, gd.product_id --, group_date  -- Group by group ID, required group size, and product ID
            )

            SELECT
                
                pn.name AS product_name,  -- Name of the product
                COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 END) AS failed_groups,  -- Count of failed groups
                COUNT(CASE WHEN fg.group_status = 'COMPLETED' THEN 1  END) AS succeeded_groups,  -- Count of succeeded groups
                fg.required_group_size AS max_group_size, -- Maximum group size for each product
            
        
                 CASE 
                    WHEN COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 END) = 1
                    THEN MAX(fg.group_size_at_failure) filter(where fg.group_status = 'FAILED')-- Return exact size when only one failed group
                    ELSE AVG(fg.group_size_at_failure)  -- Calculate average for multiple groups
                END AS avg_group_size_at_failure, 
                
                
                -- Failure rate calculation 
                CASE
                    WHEN COUNT(CASE WHEN fg.group_status = 'COMPLETED' THEN 1 ELSE NULL END) + COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 ELSE NULL END) > 0
                    THEN(CAST(COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 ELSE NULL END) AS FLOAT) 
                        / (COUNT(CASE WHEN fg.group_status = 'COMPLETED' THEN 1 ELSE NULL END) + COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 ELSE NULL END)) ) * 100
                    ELSE 0.0
                END AS failure_rate
            FROM
              failed_group_details fg
            
            JOIN
                products p ON fg.product_id = p.id  -- Join to get product name
            JOIN 
                product_names pn ON p.name_id = pn.id  -- Join to get the product's  name
            GROUP BY
                pn.name , fg.required_group_size--gd.max_group_member --, group_date  -- Group by product name, maximum group size, and group date
          HAVING
                COUNT(CASE WHEN fg.group_status = 'FAILED' THEN 1 ELSE NULL END) > 0  -- Only include results where failed groups > 0
            ORDER BY
                failure_rate DESC;  -- Order by failure rate in descending order
            """
            
            cur.execute(query_failed_group,params)
            data_failed_group = cur.fetchall()
            colnames_failed_group = [desc[0] for desc in cur.description]
            df_data_failed_group = pd.DataFrame(data_failed_group, columns= colnames_failed_group)
            
            cur.execute(query_failed_unique_group_leader,params)
            data_failed_unique_group_leader = cur.fetchall()
            colnames_failed_unique_group_leader = [desc[0] for desc in cur.description]
            df_failed_unique_group_leader = pd.DataFrame(data_failed_unique_group_leader, columns= colnames_failed_unique_group_leader)
            
            cur.execute(query_failed_unique_group_memeber,params)
            data_failed_unique_group_memeber = cur.fetchall()
            colnames_failed_unique_group_memeber = [desc[0] for desc in cur.description]
            df_failed_unique_group_memeber = pd.DataFrame(data_failed_unique_group_memeber, columns= colnames_failed_unique_group_memeber)
            
            cur.execute(query_returned_leaders_as_members,(start_date,end_date,start_date,end_date))
            data_returned_leaders_as_members = cur.fetchall()
            colnames_returned_leaders_as_members = [desc[0] for desc in cur.description]
            df_returned_leaders_as_members = pd.DataFrame(data_returned_leaders_as_members, columns= colnames_returned_leaders_as_members)
           
            cur.execute(query_returned_members_as_leaders,(start_date,end_date,start_date,end_date))
            data_returned_members_as_leaders = cur.fetchall()
            colnames_returned_members_as_leaders = [desc[0] for desc in cur.description]
            df_returned_members_as_leaders = pd.DataFrame(data_returned_members_as_leaders, columns= colnames_returned_members_as_leaders)
          
            cur.execute(query_returned_leaders_again,(start_date,end_date,start_date,end_date))
            data_returned_leaders_again = cur.fetchall()
            colnames_returned_leaders_again = [desc[0] for desc in cur.description]
            df_returned_leaders_again = pd.DataFrame(data_returned_leaders_again, columns= colnames_returned_leaders_again)   
            
            cur.execute(query_returned_members_as_members,(start_date,end_date,start_date,end_date))
            data_returned_members_as_members = cur.fetchall()
            colnames_returned_members_as_members = [desc[0] for desc in cur.description]
            df_returned_members_as_members = pd.DataFrame(data_returned_members_as_members,columns= colnames_returned_members_as_members)
            
            cur.execute(query_returned_leaders_failed_size,(start_date,end_date, start_date,end_date))
            data_returned_leaders_failed_size = cur.fetchall()
            colnames_returned_leaders_failed_size = [desc[0] for desc in cur.description]
            df_returned_leaders_failed_size = pd.DataFrame(data_returned_leaders_failed_size, columns= colnames_returned_leaders_failed_size)   
            
            
            cur.execute(query_failure_product,(start_date,end_date))
            data_failure_product = cur.fetchall()
            colnames_failure_product = [desc[0] for desc in cur.description]
            df_failure_product = pd.DataFrame(data_failure_product, columns= colnames_failure_product)  
             
            
        return df_data_failed_group, df_failed_unique_group_leader, df_failed_unique_group_memeber, df_returned_leaders_as_members, df_returned_members_as_leaders, df_returned_leaders_again,df_returned_members_as_members, df_returned_leaders_failed_size, df_failure_product
    finally:
        if conn:
            release_conn(conn)
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

# Sidebar for time frame selection
default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

df_data_failed_group, df_failed_unique_group_leader, df_failed_unique_group_memeber, df_returned_leaders_as_members, df_returned_members_as_leaders, df_returned_leaders_again, df_returned_members_as_members, df_returned_leaders_failed_size, df_failure_product = get_kpi_data(start_date,end_date)

data = {
    "Failed Group": f"{df_data_failed_group['failed_groups'].sum()}",
    "Failed Unique Group Leaders": f"{df_failed_unique_group_leader['failed_unique_group_leaders'].sum()}",
    "Failed Unique Group Members": f"{df_failed_unique_group_memeber['failed_unique_group_members'].sum()}" ,
    "Returned Members as Group Leaders": f"{df_returned_leaders_as_members['returned_leaders_as_members'].sum()}",
    "Returned Group Leaders as Members": f"{df_returned_members_as_leaders['returned_members_as_leaders'].sum()}",
    "Returned Group Leaders as Leaders Again":f"{df_returned_leaders_again['returned_leaders_again'].sum()}",
    "Returned Group Members as Members Again ":f"{df_returned_members_as_members['returned_members_as_members'].sum()}" 
}

import pandas as pd

combined_df_1 = pd.merge(df_returned_leaders_as_members, df_returned_members_as_leaders, on='time_interval', how='outer', suffixes=('_members_as_leader', '_members_as_leaders'))
combined_df_2 = pd.merge( df_returned_leaders_again, df_returned_members_as_members, on='time_interval', how='outer', suffixes=('_leader_again', '_members_as_members') )
combined_df = pd.merge(combined_df_1, combined_df_2, on='time_interval', how='outer')

# Rename columns for clarity
combined_df.columns = ['Returned Members as Group Leaders', 
                       'Time Frame',
                       'Returned Group Leaders as Members', 
                       'Returned Group Leaders as Leaders Again',
                       'Returned Group Members as Members Again'
                       ]
df = combined_df[['Time Frame','Returned Members as Group Leaders',  'Returned Group Leaders as Members', 'Returned Group Leaders as Leaders Again', 'Returned Group Members as Members Again']]
pivot_df = df.pivot_table(index=None, columns='Time Frame', values=['Returned Members as Group Leaders', 'Returned Group Leaders as Members', 'Returned Group Leaders as Leaders Again', 'Returned Group Members as Members Again'], aggfunc='sum')

# Button to generate the report
if st.sidebar.button("Generate Report"):
    # Display the KPIs
    st.write()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Failed Group", value=data["Failed Group"])
    with col2:
        st.metric(label="Failed Unique Group Leaders", value=data["Failed Unique Group Leaders"])
    with col3:
        st.metric(label="Failed Unique Group Members", value=data["Failed Unique Group Members"])
      
    st.markdown("---")
    st.subheader(f"Analysis of New vs. Recurrent Leaders from {start_date.date()} to {end_date.date()}")
    st.write("""This KPI metric provides insights into the failure rates of group leaders based on their leadership history. It categorizes leaders into two distinct groups: 

    - **New Leaders**: Leaders with no prior group creation history.
    - **Recurrent Leaders**: Leaders who have created groups previously.
    """)
    df_failed_unique_group_leader.columns = ['Failed Unique Group Leaders', 'New Leaders', 'Recurrent Leaders']
    st.dataframe(df_failed_unique_group_leader.set_index('Failed Unique Group Leaders'))
  
    
    st.markdown("---")
    
    st.subheader(f"Group Sizes at Failure from {start_date.date()} to {end_date.date()}")
    st.write("""
    This KPI metric provides insights into size of the groups that the group had at the time of failure. 
    
    The KPI tracks:
    1. **Failed Group Count**: The total number of  groups failed within the specified time frame.
    2. **Average Group Size at Failure**: The Average number of users in the group at the time of failure.
    3. **Average Required Group Size**: The Average maximum number of group size.
    """)
    df_data_failed_group.columns=['Failed Groups',  'Failed Groups at size 5 or More', 'Percentage Failed Groups at size 5 or More', 'Averege Group size at Failure', 'Averege Required Group size']
    selected_columns_1 = df_data_failed_group[['Failed Groups','Averege Group size at Failure', 'Averege Required Group size']]
    selected_columns_2 = df_data_failed_group[['Failed Groups at size 5 or More', 'Percentage Failed Groups at size 5 or More']]
   
    st.dataframe(selected_columns_1.set_index('Failed Groups'))
    st.write("""
    4. **Failed Groups at size 5 or More**: This metric represents the total number of groups that failed with five or more members. It helps to identify how many groups reached a relatively large size but still failed to achieve success.
    5. **Percentage Failed Groups at size 5 or More**: This metric calculates the percentage of failed groups that had five or more members, out of the total number of failed groups. It provides a proportion of larger failed groups relative to all failed groups, offering insight into how often failure occurs in larger groups compared to smaller ones. 
             """)
    st.dataframe(selected_columns_2.set_index('Failed Groups at size 5 or More'))
    
    st.markdown("---")
    
    # Display the time frame specific data
    st.subheader(f"Returned Members from {start_date.date()} to {end_date.date()}")
    st.write(pivot_df)
    
    # Visualization 1: Failure Rate per Product
    st.subheader(f"Failure Rate by Product from {start_date.date()} to {end_date.date()}")
    if df_failure_product['failed_groups'].sum() == 0:
        # If no failed groups, display a message
        st.warning("No failed groups found in the selected date range.")
    else:
        st.write(df_failure_product.set_index('product_name'))
        # Apply categorization for insights
        low_conversion = df_failure_product[(df_failure_product['failure_rate'] > 50) & (df_failure_product['avg_group_size_at_failure'] < 5)]
        near_success = df_failure_product[(df_failure_product['failure_rate'] > 0) & (df_failure_product['avg_group_size_at_failure'] >= (df_failure_product['max_group_size'] * 0.75))]

       
        # 1. Product Performance Insight
        st.subheader("Product Performance")
        st.write("Products with higher failure rates might require adjustments to their group deal structure (e.g., lowering the required group size).")
        st.write("By identifying products that consistently fail, focus can be directed towards marketing or operational improvements.")

        st.table(low_conversion[['product_name', 'failed_groups', 'succeeded_groups', 'failure_rate','max_group_size', 'avg_group_size_at_failure']].set_index('product_name'))

        st.subheader("Strategic Recommendation:")
        st.write("Consider lowering the required group size or improving deal terms for these products to increase conversions.")

        # 2. Group Size at Failure Insight
        st.subheader("Group Size at Failure")
        st.write("The average group size at failure provides a deeper understanding of how close these failed groups were to reaching the required size.")
        st.write("This helps identify if a slight adjustment to group size could lead to a higher success rate.")

      
        st.table(near_success[['product_name', 'failed_groups', 'succeeded_groups', 'failure_rate','max_group_size', 'avg_group_size_at_failure']].set_index('product_name'))

        st.subheader("Strategic Recommendation:")
        st.write("Consider slightly reducing the group size for these products or applying marketing incentives to push users to complete the group deal.")
    
else:
    st.info("Please enter a time frame and click 'Generate Report' to view the KPIs.")

