import requests
import pandas as pd
import streamlit as st
import datetime
import altair as alt
import plotly.express as px

FASTAPI_URL = st.secrets["fastapi"]["url"]
def fetch_data_from_api(start_date, end_date):
    try:
        response = requests.post(FASTAPI_URL, json={
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            
        })

        # Check if the request was successful
        if response.status_code != 200:
            st.error(f"API request failed with status code {response.status_code}")
            st.error(f"Response content: {response.content}")
            return None, None, None, None

        # Check if the response content is valid JSON
        try:
            data = response.json()
        except ValueError:
            st.error("Failed to decode JSON from the response")
            st.error(f"Response content: {response.content}")
            return None, None, None, None

        # Convert JSON data to DataFrames
        df = pd.DataFrame(data['aggregated_data'])
        df_total_volume_sold = pd.DataFrame(data['total_volume_sold_data'])
        df_received_orders = pd.DataFrame(data['received_orders_data'])
        df_total_revenue = pd.DataFrame(data['total_revenue_data'])

        # Ensure 'order_date' column exists before converting to datetime
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        else:
            # st.error("The 'order_date' column is missing from the aggregated data.")
            return None, None, None, None

        if 'order_date' in df_total_volume_sold.columns:
            df_total_volume_sold['order_date'] = pd.to_datetime(df_total_volume_sold['order_date'], errors='coerce')
        else:
            return None, None, None, None

        if 'order_date' in df_received_orders.columns:
            df_received_orders['order_date'] = pd.to_datetime(df_received_orders['order_date'], errors='coerce')
        else:
            st.error("The 'order_date' column is missing from the received orders data.")
            return None, None, None, None

        if 'order_date' in df_total_revenue.columns:
            df_total_revenue['order_date'] = pd.to_datetime(df_total_revenue['order_date'], errors='coerce')
        else:
            return None, None, None, None

        return df, df_total_volume_sold, df_received_orders, df_total_revenue

    except requests.exceptions.RequestException as e:
        st.error(f"An error occurred while making the API request: {e}")
        return None, None, None, None

def aggregate_by_payment_method(data, frequency):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        if frequency == 'Weekly':
            data['frequency'] = data['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
        elif frequency == 'Monthly':
            data['frequency'] = data['order_date'].dt.to_period('M').apply(lambda r: r.start_time)
        else:
            data['frequency'] = data['order_date'].dt.date  # Show only the date for daily frequency
        
        aggregated_data = data.groupby(['frequency', 'payment_method']).agg({
            'total_orders': 'sum'
        }).reset_index()
        
        return aggregated_data

def apply_filters(data, start_date, end_date, order_type_filter, payment_method_filter):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        data['order_date'] = pd.to_datetime(data['order_date'])
        # Convert start_date and end_date to datetime64
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Filter by date range
        filtered_data = data[(data['order_date'].between(start_date, end_date))]
        
        # Filter by order type
        if order_type_filter != 'ALL':
            filtered_data = filtered_data[filtered_data['payment_method'] == order_type_filter]
        
        # Filter by payment method
        if payment_method_filter != 'ALL':
            filtered_data = filtered_data[filtered_data['payment_method'] == payment_method_filter]
        
        return filtered_data
def create_altair_chart(data):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        melted_data = data.melt(id_vars=['frequency'], value_vars=['total_orders','total_accepted_orders', 'group_order_count','completed_group_order_count', 'personal_order_count'],
                                var_name='Order Type', value_name='Order Count')
        
        chart = alt.Chart(melted_data).mark_line(point=True).encode(
            x='frequency:T',
            y='Order Count:Q',
            color='Order Type:N'
        ).properties(
            title='Orders Over Time'
        ).interactive()

        return chart

def create_payment_method_chart(data):
    chart = alt.Chart(data).mark_line(point=True).encode(
        x='frequency:T',
        y='total_orders:Q',
        color='payment_method:N'
    ).properties(
        title='Payment Method Distribution Over Time'
    ).interactive()

    return chart

def aggregate_by_frequency(data, frequency):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        if frequency == 'Weekly':
            data['frequency'] = data['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
        elif frequency == 'Monthly':
            data['frequency'] = data['order_date'].dt.to_period('M').apply(lambda r: r.start_time)
        else:
            data['frequency'] = data['order_date'].dt.date  # Show only the date for daily frequency
        
        aggregated_data = data.groupby(['frequency']).agg({
            'total_orders': 'sum',
            'total_accepted_orders':'first',
            'group_order_count': 'sum',
            'completed_group_order_count':'sum',
            'personal_order_count': 'sum'
        }).reset_index()
     
       
        return aggregated_data

def aggregate_metrics_by_frequency(data, frequency, metric_name):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        if frequency == "Weekly":
            data = data.resample('W-Mon', on='order_date').sum().reset_index().sort_values('order_date')
            data['week'] = data['order_date'].dt.strftime('%Y-%U')
            aggregated_data = data[['week', metric_name]].rename(columns={'week': 'frequency'})
        elif frequency == "Daily":
            data['day'] = data['order_date'].dt.strftime('%Y-%m-%d')
            aggregated_data = data.groupby('day')[metric_name].sum().reset_index().rename(columns={'day': 'frequency'})
        elif frequency == "Monthly":
            data['month'] = data['order_date'].dt.strftime('%Y-%m')
            aggregated_data = data.groupby('month')[metric_name].sum().reset_index().rename(columns={'month': 'frequency'})
        else:
            aggregated_data = pd.DataFrame()  # Handle unexpected frequency value
        return aggregated_data


def create_payment_method_chart(data):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
    else:
        chart = alt.Chart(data).mark_line(point=True).encode(
            x='frequency:T',
            y='total_orders:Q',
            color='payment_method:N'
        ).properties(
            title='Payment Method Distribution Over Time'
        ).interactive()

        return chart

def aggregate_by_week(data, metric_name, start_date, end_date):
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")

    else:
        # Convert 'order_date' to datetime
        data.loc[:, 'order_date'] = pd.to_datetime(data['order_date'])
        
        # Filter data within the selected date range
        data = data[(data['order_date'] >= start_date) & (data['order_date'] <= end_date)]
        
        # Adjust to ensure the week starts on Wednesday and ends on Tuesday
        data['order_date'] -= pd.to_timedelta((data['order_date'].dt.weekday + 1) % 7, unit='d')
        
        # Calculate the last full week ending on Tuesday
        current_date = pd.to_datetime('today')
        last_full_week_end = current_date - pd.to_timedelta((current_date.weekday() + 1) % 7, unit='d') - pd.DateOffset(days=1)
        
        # Initialize the full weeks range
        start_date_aligned = start_date - pd.to_timedelta((start_date.weekday() + 1) % 7, unit='d')
        full_weeks = pd.date_range(start=start_date_aligned, end=last_full_week_end, freq='W-WED')
        
        # Resample data to weekly, summing the specified metric
        aggregated_data = data.set_index('order_date').resample('W-WED')[metric_name].sum().reset_index()
        
        # Filter data to include only weeks up to the last full week
        aggregated_data = aggregated_data[aggregated_data['order_date'] <= last_full_week_end]
        
        # Merge the full range with the data to ensure all weeks are present
        full_data = pd.DataFrame({'order_date': full_weeks})
        aggregated_data = pd.merge(full_data, aggregated_data, on='order_date', how='left').fillna(0)
        
        # Ensure data is sorted by the actual date before creating week ranges
        aggregated_data = aggregated_data.sort_values('order_date')
        
        # Generate week ranges
        aggregated_data['week_start'] = aggregated_data['order_date'].dt.strftime('%b %d')
        aggregated_data['week_end'] = (aggregated_data['order_date'] + pd.DateOffset(days=6)).dt.strftime('%b %d')
        aggregated_data['week'] = aggregated_data['week_start'] + ' to ' + aggregated_data['week_end']
        
        # Return the final aggregated data with the specified metric
        result_data = aggregated_data[['week', metric_name]].rename(columns={'week': 'frequency'})
    
        return result_data
def aggregate_by_day(data, metric_name, start_date, end_date):
  
    if data is None or data.empty:
        st.warning("No data available for the selected date range.")
       
    else:
        data['order_date'] = pd.to_datetime(data['order_date'])
        data = data[(data['order_date'] >= start_date) & (data['order_date'] <= end_date)]
        
        data = data.set_index('order_date').resample('D').sum().reset_index()
        
        full_days = pd.date_range(start=start_date, end=end_date, freq='D')
        
        full_data = pd.DataFrame({'order_date': full_days})
        data = pd.merge(full_data, data, on='order_date', how='left').fillna(0)
        
        data = data.sort_values('order_date')
        data['day'] = data['order_date'].dt.strftime('%b %d')
        
        aggregated_data = data[['day', metric_name]].rename(columns={'day': 'frequency'})
    
        return aggregated_data


def aggregate_by_month(data, metric_name,start_date,end_date):

    if data is None or data.empty:
        st.warning("No data available for the selected date range.")

    if data is not None and not data.empty:
        data.loc[:, 'order_date'] = pd.to_datetime(data['order_date'])

        data = data[data['order_date'] >= '2023-10-01']
        
        data = data.set_index('order_date').resample('M').sum().reset_index()
        
        full_months = pd.date_range(start=start_date, end=end_date, freq='M')
        
        full_data = pd.DataFrame({'order_date': full_months})
        data = pd.merge(full_data, data, on='order_date', how='left').fillna(0)
        
        data = data.sort_values('order_date')
        data['month'] = data['order_date'].dt.strftime('%B')
        
        aggregated_data = data[['month', metric_name]].rename(columns={'month': 'frequency'})
    
        return aggregated_data


def show_trend_view(metric_name, metric_data, frequency,start_date, end_date):
    # Aggregate data by week
    if metric_data is None or metric_data.empty:
        st.warning("No data available for the selected date range.")
    else:
        if frequency == 'Weekly':
            aggregated_data = aggregate_by_week(metric_data, metric_name,start_date, end_date)
        elif frequency == 'Monthly':
            aggregated_data = aggregate_by_month(metric_data, metric_name,start_date, end_date)
        else:
            aggregated_data = aggregate_by_day(metric_data, metric_name,start_date, end_date)
        
        # Plotting using Plotly Express
        fig = px.line(aggregated_data, x='frequency', y=metric_name, title=f'{metric_name.replace("_", " ").title()}{" "}{frequency} Trends')
        # Adjust layout width and x-axis properties
        fig.update_layout(
            autosize=False,
            width=1200,  # Adjust the width as needed
            height=600,  # Adjust the height as needed
            margin=dict(l=40, r=40, b=40, t=40)
        )
        
        # Customize x-axis
        fig.update_xaxes(
            tickangle=300,
            tickformat='%d %b',
            tickmode='linear',  # Ensure all ticks are displayed
            dtick=1,  # Set the interval between ticks
            title=frequency
        )
        
        # Customize y-axis
        fig.update_yaxes(title=metric_name.replace('_', ' ').title())
        
        # Add markers to the line
        fig.update_traces(mode='lines+markers')
        
        # Display the chart in Streamlit
        st.plotly_chart(fig)

# Sidebar for user inputs
st.sidebar.title("Filters")

# Date range filter
default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Order type and payment method filters
order_type_filter = st.sidebar.selectbox("Order Type", ["ALL", "Individual", "Group"])

# Frequency filter
frequency = st.sidebar.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])


# Fetch the aggregated data
aggregated_data, total_volume_sold_data, received_orders_data, total_revenue_data = fetch_data_from_api(start_date, end_date)

# Payment Method filter 
if aggregated_data is not None and not aggregated_data.empty:
    payment_method_filter = st.sidebar.selectbox(
        "Payment Method", options=['ALL'] + list(aggregated_data['payment_method'].unique())
    )

tab1, tab2 = st.tabs(["Live Order Overview", "Order Weekly Report"])
# Apply filters button
if st.sidebar.button("Filter"):

    # Ensure the dataframes are not None and contain data before proceeding
    if total_volume_sold_data is not None and not total_volume_sold_data.empty:
        total_volume_sold_data.loc[:, 'order_date'] = pd.to_datetime(total_volume_sold_data['order_date'])
    else:
        st.warning("No data available for total volume sold.")
        total_volume_sold_filtered = pd.DataFrame()  # Initialize an empty DataFrame if no data

    if total_revenue_data is not None and not total_revenue_data.empty:
        total_revenue_data.loc[:, 'order_date'] = pd.to_datetime(total_revenue_data['order_date'])
    else:
        st.warning("No data available for total revenue.")
        total_revenue_filtered = pd.DataFrame()  # Initialize an empty DataFrame if no data

    if received_orders_data is not None and not received_orders_data.empty:
        received_orders_data.loc[:, 'order_date'] = pd.to_datetime(received_orders_data['order_date'])
    else:
        st.warning("No data available for received orders.")
        received_orders_filtered = pd.DataFrame()  # Initialize an empty DataFrame if no data

    # Filter and aggregate the newly added metrics, only if there's data
    if total_volume_sold_data is not None and not total_volume_sold_data.empty:
        total_volume_sold_filtered = total_volume_sold_data[
            total_volume_sold_data['order_date'].between(start_date, end_date)
        ]
    else:
        total_volume_sold_data = pd.DataFrame()  # Handle no data case gracefully

    if total_revenue_data is not None and not total_revenue_data.empty:
        total_revenue_filtered = total_revenue_data[
            total_revenue_data['order_date'].between(start_date, end_date)
        ]
    else:
        total_revenue_data = pd.DataFrame()  # Handle no data case gracefully

    if received_orders_data is not None and not received_orders_data.empty:
        received_orders_filtered = received_orders_data[
            received_orders_data['order_date'].between(start_date, end_date)
        ]
    else:
        received_orders_data = pd.DataFrame()  # Handle no data case gracefully

    metrics_data = {
    'Total Revenue': f"{total_revenue_filtered['total_revenue'].sum() if total_revenue_filtered is not None and not total_revenue_filtered.empty else 0}",
    'Group Revenue': f"{total_revenue_filtered['group_revenue'].sum() if total_revenue_filtered is not None and not total_revenue_filtered.empty else 0}",
    'Personal Revenue': f"{total_revenue_filtered['personal_revenue'].sum() if total_revenue_filtered is not None and not total_revenue_filtered.empty else 0}",
    
    'Total Received Orders': f"{received_orders_filtered['total_received_orders'].sum() if received_orders_filtered is not None and not received_orders_filtered.empty else 0}",
    'Personal Received Orders': f"{received_orders_filtered['personal_order_recieved'].sum() if received_orders_filtered is not None and not received_orders_filtered.empty else 0}",
    'Group Received Orders': f"{received_orders_filtered['group_order_recieved'].sum() if received_orders_filtered is not None and not received_orders_filtered.empty else 0}",
    
    'Total Volume Sold': f"{total_volume_sold_filtered['total_volume_sold'].sum() if total_volume_sold_filtered is not None and not total_volume_sold_filtered.empty else 0:.2f}",
    'Personal Volume Sold': f"{total_volume_sold_filtered['personal_volume_sold'].sum() if total_volume_sold_filtered is not None and not total_volume_sold_filtered.empty else 0:.2f}",
    'Group Volume Sold': f"{total_volume_sold_filtered['group_volume_sold'].sum() if total_volume_sold_filtered is not None and not total_volume_sold_filtered.empty else 0:.2f}"
    }

    
    with tab2:
        st.markdown(f"""
        <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
            <h3>Total Revenue</h3>
            <p>from {start_date.date()} to {end_date.date()}</p>
            <p>Total Revenue: {metrics_data['Total Revenue']} Birr</p>
            <h4>Personal Revenue</h4>
            <p>Personal Revenue: {metrics_data['Personal Revenue']}Birr</p>
            <h4>Group Revenue</h4>
            <p>Group Revenue: {metrics_data['Group Revenue']}Birr</p>       
        </div>
        """, unsafe_allow_html=True)
        if total_revenue_data is not None and not total_revenue_data.empty:
            total_revenue_data['total_revenue']  = pd.to_numeric(total_revenue_data['total_revenue'])
            
        with st.expander("Total Revenue Trend"):
            show_trend_view('total_revenue', total_revenue_data,frequency,start_date, end_date)
        
        st.markdown(f"""
        <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
            <h3>Total Received Orders</h3>
            <p>from {start_date.date()} to {end_date.date()}</p>
            <p>Total Received Orders: {metrics_data['Total Received Orders']}</p>
            <h4>Personal Received Orders</h4>
            <p>Personal Received Orders: {metrics_data['Personal Received Orders']}</p>
            <h4>Group Received Orders</h4>
            <p>Group Received Orders: {metrics_data['Group Received Orders']}</p>
        
        </div>
        """, unsafe_allow_html=True)
        with st.expander("Total Received Orders Trend"):
            show_trend_view('total_received_orders', received_orders_data,frequency,start_date, end_date)
        
        st.markdown(f"""
        <div style="background-color: #e0f7fa; padding: 20px; border-radius: 5px;">
            <h3>Total Volume Sold</h3>
            <p>from {start_date.date()} to {end_date.date()}</p>
            <p>Value: {metrics_data['Total Volume Sold']} kg</p>
            <h4>Personal Volume Sold</h4>
            <p>Personal Volume Sold: {metrics_data['Personal Volume Sold']}</p>
            <h4>Group Volume Sold</h4>
            <p>Group Volume Sold: {metrics_data['Group Volume Sold']}</p>
        
        </div>
        """, unsafe_allow_html=True)
        if total_volume_sold_data is not None and not total_volume_sold_data.empty:
            total_volume_sold_data['total_volume_sold']  = pd.to_numeric(total_volume_sold_data['total_volume_sold'])
    
        with st.expander("Total Volume Sold Trend"):
            show_trend_view('total_volume_sold', total_volume_sold_data,frequency,start_date, end_date)
    with tab1:
        # Live Order Overview
        # Check if payment_method_filter is defined, if not set a default
        if 'payment_method_filter' not in locals():
            payment_method_filter = None  # Or set a  default, e.g., 'All'

        filtered_data = apply_filters(aggregated_data, start_date, end_date, order_type_filter, payment_method_filter)
        if filtered_data is None or filtered_data.empty:
            st.warning("No data available for the selected date range.")
        else:
            st.write(f"Total Orders from {start_date.date()} to {end_date.date()}: {filtered_data['total_orders'].sum()}")
            st.write(f"Total Accepted orders from {start_date.date()} to {end_date.date()} : {filtered_data['total_accepted_orders'].unique().sum()}")
            st.write(f"Group Orders from {start_date.date()} to {end_date.date()}: {filtered_data['group_order_count'].sum()}")
            st.write(f"Completed Group Order from {start_date.date()} to {end_date.date()}: {filtered_data['completed_group_order_count'].sum()}")
            st.write(f"Personal Orders from {start_date.date()} to {end_date.date()}: {filtered_data['personal_order_count'].sum()}")
        
            aggregated_data_frequency = aggregate_by_frequency(filtered_data, frequency)
        
            chart = create_altair_chart(aggregated_data_frequency)
            
            st.altair_chart(chart, use_container_width=True)
            
            payment_method_data = aggregate_by_payment_method(filtered_data, frequency)
            payment_method_chart = create_payment_method_chart(payment_method_data)
            
            st.altair_chart(payment_method_chart, use_container_width=True)
            
            st.write("Order Activity Overview Table")
      
            st.dataframe(aggregated_data_frequency.rename(columns={
                'frequency': 'Date',
                'total_orders': 'Total Orders',
                'total_accepted_orders': 'Total Accepted Orders',
                'group_order_count': 'Group Order Count',
                'completed_group_order_count': 'Completed Group Order Count',
                'personal_order_count': 'Personal Order Count'
            
            }).set_index('Date'), height=400, width=1000)