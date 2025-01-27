import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import altair as alt
from datetime import datetime, timedelta
from db_pool import get_conn, release_conn
# Function to get data from the database
@st.cache_data
def get_data(query):
    conn = get_conn()
    if not conn:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        release_conn(conn)

st.sidebar.success("Select KPI above.")
st.sidebar.header("Users KPI")

default_start_date = datetime.today() - timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)
frequency = st.sidebar.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])

# Determine SQL date part based on frequency
if frequency == "Daily":
    date_part = "DAY"
elif frequency == "Weekly":
    date_part = "WEEK"
else:
    date_part = "MONTH"

# Description of the logic
st.write("""
### User KPIs Dashboard
This dashboard provides insights into user behavior and engagement, focusing on the most loyal customers, user distribution by gender and age. 
The data is filtered to include only verified users and completed orders.
- **Verified Users**: Users with `user_status` marked as `verified`.
- **Completed Orders**: Orders with the `status` marked as `Completed`.
- **Date Range**: Orders placed between the selected date range.
- **Frequency**: Data aggregation based on the selected frequency (Daily, Weekly, Monthly).
""")

# SQL Queries with date filter, frequency aggregation, and status filters
query_loyalty = f"""
    SELECT 
        u.id AS user_id, 
        u.name, 
        u.phone,
        u.user_status,
        SUM(o.total_amount) AS total_spent, 
        COUNT(o.id) AS total_orders
    FROM 
        users u
    LEFT JOIN 
        groups_carts gc ON u.id = gc.user_id
    LEFT JOIN 
        orders o ON gc.id = o.groups_carts_id
    WHERE 
        o.created_at BETWEEN '{start_date}' AND '{end_date}'
        AND u.user_status = 'VERIFIED'
        AND o.status = 'COMPLETED'
    GROUP BY 
        u.id, u.name, u.phone
    ORDER BY 
        total_spent DESC
    LIMIT 10;
"""

query_gender = f"""
    SELECT 
        gender, 
        COUNT(*) AS count
    FROM 
        users
    WHERE 
        created_at BETWEEN '{start_date}' AND '{end_date}'
        AND user_status = 'VERIFIED'
    GROUP BY 
        gender;
"""

query_age = f"""
    SELECT 
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 18 AND 25 THEN '18-25'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 26 AND 35 THEN '26-35'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 36 AND 45 THEN '36-45'
            ELSE '46+'
        END AS age_bracket,
        COUNT(*) AS count
    FROM 
        users
    WHERE 
        created_at BETWEEN '{start_date}' AND '{end_date}'
        AND user_status = 'VERIFIED'
    GROUP BY 
        age_bracket;
"""

query_gender_age = f"""
    SELECT 
        gender,
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 18 AND 25 THEN '18-25'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 26 AND 35 THEN '26-35'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, user_birthday)) BETWEEN 36 AND 45 THEN '36-45'
            ELSE '46+'
        END AS age_bracket,
        COUNT(*) AS count
    FROM 
        users
    WHERE 
        created_at BETWEEN '{start_date}' AND '{end_date}'
        AND user_status = 'VERIFIED'
    GROUP BY 
        gender, age_bracket;
"""
query_device = f"""
    SELECT 
        user_id,
        os,
        created_at
    FROM 
        devices
    WHERE 
        created_at BETWEEN '{start_date}' AND '{end_date}';
"""
# Load data
df_loyalty = get_data(query_loyalty)
df_gender = get_data(query_gender)
df_age = get_data(query_age)
df_gender_age = get_data(query_gender_age)
df_device = get_data(query_device)


# Function to visualize OS distribution
@st.cache_data
def visualize_os_distribution(df, selected_date_range):
    start_date, end_date = selected_date_range
    
    # Ensure 'created_at' is a datetime column
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # Filter DataFrame for orders within the selected date range
    filtered_data = df[(df['created_at'] >= start_date) & 
                       (df['created_at'] <= end_date)]
    
    if filtered_data.empty:
        st.markdown(f"No data available for the selected date range.")
        return
    
    # Count the occurrences of each OS
    os_counts = filtered_data['os'].value_counts().reset_index()
    os_counts.columns = ['OS', 'Count']
    
    # Create an Altair bar chart
    chart = alt.Chart(os_counts).mark_bar().encode(
        x='OS',
        y='Count',
        color=alt.Color('OS', scale=alt.Scale(range=['blue', 'green'])),
        tooltip=['OS', 'Count']
    ).properties(
        title=f'OS Distribution from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'
    ).interactive()
    
    # Display the chart
    st.altair_chart(chart, use_container_width=True)
# Apply filters button
if st.sidebar.button("Filter"):    
    # Streamlit App
    st.title('User KPIs Dashboard')
    # Drop 'user_id' and 'user_status' columns from the dataframe before displaying
    df_loyalty_display = df_loyalty.drop(columns=['user_id','user_status'])
    st.header('Top Loyal Customers')
    st.dataframe(df_loyalty_display)

    # Plotting Total Spending by Users
    st.header('Total Spending by Top Loyal Users')
    fig, ax = plt.subplots()
    sns.barplot(data=df_loyalty, x='name', y='total_spent', ax=ax)
    ax.set_title('Total Spending by Top Loyal Users')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    st.pyplot(fig)

    # Plotting User Distribution by Gender
    st.header('User Distribution by Gender')
    fig, ax = plt.subplots()
    sns.barplot(data=df_gender, x='gender', y='count', ax=ax)
    ax.set_title('User Distribution by Gender')
    st.pyplot(fig)

    # Plotting User Distribution by Age
    st.header('User Distribution by Age')
    fig, ax = plt.subplots()
    sns.barplot(data=df_age, x='age_bracket', y='count', ax=ax)
    ax.set_title('User Distribution by Age')
    st.pyplot(fig)

    # Plotting User Distribution by Gender and Age
    st.header('User Distribution by Gender and Age')
    fig, ax = plt.subplots()
    sns.barplot(data=df_gender_age, x='age_bracket', y='count', hue='gender', ax=ax)
    ax.set_title('User Distribution by Gender and Age')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    st.pyplot(fig)
    # Visualizing OS Distribution
    st.header('OS Distribution')
    visualize_os_distribution(df_device, (start_date, end_date))
