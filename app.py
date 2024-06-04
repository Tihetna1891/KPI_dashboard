import requests
import pandas as pd
import streamlit as st
import altair as alt
from map import *

# Sidebar inputs
start_date, end_date, time_frame = sidebar()

# Load data from the database
orders = get_orders(start_date, end_date)
products = get_product()
product_names = get_product_names()

products = products.merge(product_names, on='name_id')

total_sales = calculate_total_sales(orders, products, time_frame)
order_volume = calculate_order_volume(orders, products, time_frame)
average_order_value = calculate_average_order_value(orders, products, time_frame)
fulfillment_time = calculate_fulfillment_time(orders, products, time_frame)
product_popularity_data = product_popularity(orders, products, time_frame)

total_sales_vendor=calculate_total_sales_vendors(orders, products, time_frame)
order_volume_vendor=calculate_order_volume_vendor(orders, products, time_frame)
average_order_value_vendor=calculate_average_order_value_vendor(orders, products, time_frame)
fulfillment_time_vendor = calculate_fulfillment_time_vendor(orders, products, time_frame)
product_popularity_data_vendor = product_popularity_vendor(orders, products, time_frame)

# Summarize the most sold products
most_sold_products = total_sales.groupby('product_name')['total_sales'].sum().reset_index().sort_values(by='total_sales', ascending=False)
sorted_product_names = most_sold_products['product_name'].tolist()

# Get unique vendor names for selection
unique_vendor_names = products['vendor_name'].unique()
selected_vendors = st.sidebar.multiselect("Select vendors", unique_vendor_names, default=unique_vendor_names)

# Display products sorted by most sold
selected_products = st.sidebar.multiselect("Select the most sold products", sorted_product_names, default=sorted_product_names)

# Filter data based on selected vendors
total_sales = total_sales[total_sales['vendor_name'].isin(selected_vendors) & (total_sales['product_name'].isin(selected_products))]
order_volume = order_volume[order_volume['vendor_name'].isin(selected_vendors) & (order_volume['product_name'].isin(selected_products))]
average_order_value = average_order_value[average_order_value['vendor_name'].isin(selected_vendors) & (average_order_value['product_name'].isin(selected_products))]
fulfillment_time = fulfillment_time[fulfillment_time['vendor_name'].isin(selected_vendors) & (fulfillment_time['product_name'].isin(selected_products))]
product_popularity_data = product_popularity_data[product_popularity_data['vendor_name'].isin(selected_vendors) & (product_popularity_data['product_name'].isin(selected_products))]

total_sales_vendor = total_sales_vendor[total_sales_vendor['vendor_name'].isin(selected_vendors) ]
order_volume_vendor = order_volume_vendor[order_volume_vendor['vendor_name'].isin(selected_vendors)]
average_order_value_vendor= average_order_value_vendor[average_order_value_vendor['vendor_name'].isin(selected_vendors)]
fulfillment_time_vendor = fulfillment_time_vendor[fulfillment_time_vendor['vendor_name'].isin(selected_vendors)]
product_popularity_data_vendor = product_popularity_data_vendor[product_popularity_data_vendor['vendor_name'].isin(selected_vendors)]
# Streamlit dashboard
st.title('Vendor Performance KPI')

# Summary section
st.subheader("Summary")
st.markdown(f"**Total Sales:** {total_sales['total_sales'].sum():,.2f}")
st.markdown(f"**Total Orders:** {order_volume['order_count'].sum()}")
st.markdown(f"**Average Order Value:** {average_order_value['average_order_value'].mean():,.2f}")

top_products = most_sold_products.head(5)
st.markdown("**Top 5 Most Sold Products:**")
for i, row in top_products.iterrows():
    st.markdown(f"- {row['product_name']}: {row['total_sales']:,.2f}")

# Metrics Descriptions
st.markdown("## Metrics Descriptions")
st.markdown("""
### Total Sales
This metric calculates the total revenue generated from sales over a specified period. It is calculated by summing the sales revenue for each product sold.

### Order Volume
This metric measures the total number of COMPLETED orders placed over a specified period. It indicates the quantity of products sold.

### Average Order Value
This metric calculates the average revenue per order. It is calculated by dividing the total sales revenue by the number of orders.

### Fulfillment Time
This metric measures the average time taken to fulfill orders from the time they are placed to the time they are delivered. It indicates the efficiency of the fulfillment process.

### Product Popularity
This metric measures the popularity of each product based on the number of units sold. It helps identify the best-selling products.


### Product Popularity
This metric measures the popularity of each product based on the number of units sold. It helps identify the best-selling products.
""")

# Define a custom color palette
custom_colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
    '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
]
# Visualization for each metric

# Total Sales

st.header('Total Sales by Vendor')
sales_chart = alt.Chart(total_sales_vendor).mark_line().encode(
    x='date:T',
    y='total_sales:Q',
    color='vendor_name:N',
    tooltip=['vendor_name', 'date', 'total_sales']
).interactive().properties(title='Total Sales Over Time')

st.altair_chart(sales_chart, use_container_width=True)
total_sales['vendor_product'] = total_sales['vendor_name'] + ' - ' + total_sales['product_name']
# Loop through each selected vendor and create separate charts in expandable sections
with st.expander("Expand to view Total Sales of each Product by each Vendor"):
    for vendor in selected_vendors:
        st.subheader(f'Total Sales for {vendor}')
        vendor_data = total_sales[total_sales['vendor_name'] == vendor]
        
        if not vendor_data.empty:
            vendor_data['vendor_product'] = vendor_data['product_name']
            
            sales_chart = alt.Chart(vendor_data).mark_line().encode(
                x='date:T',
                y='total_sales:Q',
                color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                tooltip=['vendor_name', 'product_name', 'date', 'total_sales']
            ).interactive().properties(
                title=f'Total Sales Over Time for {vendor}'
            )
            
            st.altair_chart(sales_chart, use_container_width=True)
        else:
            st.write(f"No data available for {vendor}")
            
# Order Volume

st.header('Order Volume by Vendor')
volume_chart = alt.Chart(order_volume_vendor).mark_line().encode(
    x=alt.X('date:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
    y='order_count:Q',
    color='vendor_name:N',
    tooltip=['vendor_name', 'date', 'order_count']
).interactive().properties(title='Order Volume Over Time')
st.altair_chart(volume_chart, use_container_width=True)
order_volume['vendor_product'] = order_volume['vendor_name'] + ' - ' + order_volume['product_name']
with st.expander("Expand to view Order Volume of each Product by each Vendor"):
    for vendor in selected_vendors:
        st.subheader(f'Order Volume for {vendor}')
        vendor_data = order_volume[order_volume['vendor_name'] == vendor]
        
        if not vendor_data.empty:
            vendor_data['vendor_product'] = vendor_data['product_name']
            
            volume_chart = alt.Chart(vendor_data).mark_line().encode(
                x='date:T',
                y='order_count:Q',
                color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                tooltip=['vendor_name', 'product_name', 'date', 'order_count']
            ).interactive().properties(
                title=f'Order Volume Over Time for {vendor}'
            )
            
            st.altair_chart(volume_chart, use_container_width=True)
        else:
            st.write(f"No data available for {vendor}")

# Average Order Value
st.header('Average Order Value by Vendor')
aov_chart = alt.Chart(average_order_value_vendor).mark_line().encode(
    x='date:T',
    y='average_order_value:Q',
    color='vendor_name:N',
    tooltip=['vendor_name', 'date', 'average_order_value']
).interactive().properties(title='Average Order Value Over Time')
st.altair_chart(aov_chart, use_container_width=True)
average_order_value['vendor_product'] = average_order_value['vendor_name'] + ' - ' + average_order_value['product_name']
with st.expander("Expand to view Average Order Value of each Product by each Vendor"):
    for vendor in selected_vendors:
        st.subheader(f'Average Order Value for {vendor}')
        vendor_data = average_order_value[average_order_value['vendor_name'] == vendor]
        
        if not vendor_data.empty:
            vendor_data['vendor_product'] = vendor_data['product_name']
            
            aov_chart = alt.Chart(vendor_data).mark_line().encode(
                x='date:T',
                y='average_order_value:Q',
                color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                tooltip=['vendor_name', 'product_name', 'date', 'average_order_value']
            ).interactive().properties(
                title=f'Average Order Value Over Time for {vendor}'
            )
            
            st.altair_chart(aov_chart, use_container_width=True)
        else:
            st.write(f"No data available for {vendor}")

# Fulfillment Time
st.header('Fulfillment Time by Vendor')
fulfillment_time_chart = alt.Chart(fulfillment_time_vendor).mark_line().encode(
    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
    y='fulfillment_time:Q',
    color='vendor_name:N',
    tooltip=['vendor_name', 'fulfillment_time']
).interactive().properties(title='Fulfillment Time by Vendor')
st.altair_chart(fulfillment_time_chart, use_container_width=True)
fulfillment_time['vendor_product'] = fulfillment_time['vendor_name'] + ' - ' + fulfillment_time['product_name']
with st.expander("Expand to view Fulfillment Time of each Product by each Vendor"):
    for vendor in selected_vendors:
        st.subheader(f'Fulfillment Time for {vendor}')
        vendor_data = fulfillment_time[fulfillment_time['vendor_name'] == vendor]
        
        if not vendor_data.empty:
            vendor_data['vendor_product'] = vendor_data['product_name']
            
            fulfillment_time_chart = alt.Chart(vendor_data).mark_line().encode(
                x='time_frame:T',
                y='fulfillment_time:Q',
                color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                tooltip=['vendor_name', 'product_name', 'fulfillment_time']
            ).interactive().properties(
                title=f'Fulfillment Time Over Time for {vendor}'
            )
            
            st.altair_chart(fulfillment_time_chart, use_container_width=True)
        else:
            st.write(f"No data available for {vendor}")

# Product Popularity
st.header('Product Popularity by Vendor')
product_popularity_data_chart = alt.Chart(product_popularity_data_vendor).mark_line().encode(
    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
    y='product_popularity:Q',
    color='vendor_name:N',
).interactive().properties(title='Product Popularity by Vendor')
st.altair_chart(product_popularity_data_chart, use_container_width=True)
product_popularity_data['vendor_product'] = product_popularity_data['vendor_name'] + ' - ' + product_popularity_data['product_name']
with st.expander("Expand to view Product Popularity of each Product by each Vendor"):
    for vendor in selected_vendors:
        st.subheader(f'Product Popularity by {vendor}')
        vendor_data = product_popularity_data[product_popularity_data['vendor_name'] == vendor]
        
        if not vendor_data.empty:
            vendor_data['vendor_product'] = vendor_data['product_name']
            
            popularity_chart = alt.Chart(vendor_data).mark_line().encode(
                x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                y='product_popularity:Q',
                color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                tooltip=['vendor_name', 'product_name', 'product_popularity']
            ).interactive().properties(
                title=f'Product Popularity Over Time for {vendor}'
            )
            
            st.altair_chart(popularity_chart, use_container_width=True)
        else:
            st.write(f"No data available for {vendor}")