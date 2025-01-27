import datetime
import pandas as pd
import streamlit as st
import altair as alt
from map import *
import matplotlib.pyplot as plt
import seaborn as sns

st.sidebar.success("Select KPI above.")
st.markdown("# Vendor Performance KPI")
st.sidebar.header("Vendor Performance KPI")

# Sidebar inputs
default_start_date = datetime.datetime.today() - datetime.timedelta(days=7)
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", datetime.datetime.today())
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)
time_frame = st.sidebar.selectbox("Select time frame", ["Daily", "Weekly", "Monthly", "Yearly"])

# Load data from the database
orders = get_orders(start_date, end_date)
products = get_products()
product_names = get_product_names()
categories = get_categories()
vendors = get_vendors()

# Merge products with product names and categories
product_names = product_names.merge(categories, on='category_id')
products = products.merge(product_names, on='name_id')

category_sales = calculate_category_sales(orders, products, time_frame)
# Calculate total sales, order volume, and average order value
total_sales = calculate_total_sales(orders, products, time_frame)
order_volume = calculate_order_volume(orders, products, time_frame)
average_order_value = calculate_average_order_value(orders, products, time_frame)

# Calculate metrics by vendor
category_sales_vendor=calculate_category_sales_vendors(orders, products, time_frame)
total_sales_vendor = calculate_total_sales_vendors(orders, products, time_frame)
order_volume_vendor = calculate_order_volume_vendor(orders, products, time_frame)
average_order_value_vendor = calculate_average_order_value_vendor(orders, products, time_frame)
product_portfolio = calculate_product_portfolio(orders, products,time_frame)
# Product popularity data
product_sales_data = product_sales(orders, products, time_frame)
product_sales_data_vendor = product_sales_vendor(orders, products, time_frame)


# Summarize the most sold products
most_sold_products_all = total_sales.groupby('product_name')['total_sales'].sum().reset_index().sort_values(by='total_sales', ascending=False)
sorted_product_names_all = most_sold_products_all['product_name'].tolist()

# Get unique vendor names for selection
unique_vendor_names = products['vendor_name'].unique()
selected_vendors = st.sidebar.multiselect("Select vendors", unique_vendor_names, default=unique_vendor_names)

# Filter the products based on the selected vendors
if len(selected_vendors) == len(unique_vendor_names):
    # If all vendors are selected, include all products
    available_product_names = products['product_name'].unique()
else:
    # Otherwise, filter products based on selected vendors
    filtered_products = products[products['vendor_name'].isin(selected_vendors)]
    available_product_names = filtered_products['product_name'].unique()
most_sold_products = total_sales[total_sales['product_name'].isin(available_product_names)] \
    .groupby('product_name')['total_sales'].sum() \
    .reset_index() \
    .sort_values(by='total_sales', ascending=False)

sorted_product_names = most_sold_products['product_name'].tolist()
# Use a single multiselect for product selection
selected_products = st.sidebar.multiselect("Select products", sorted_product_names, default=sorted_product_names)

if st.sidebar.button("Filter"):
   
    # Filter data based on selected vendors and products
    category_sales['category_sales'] = pd.to_numeric(category_sales['category_sales'], errors='coerce')
    total_sales = total_sales[total_sales['vendor_name'].isin(selected_vendors) & total_sales['product_name'].isin(selected_products)]
    total_sales['total_sales'] = pd.to_numeric(total_sales['total_sales'], errors='coerce')
    order_volume = order_volume[order_volume['vendor_name'].isin(selected_vendors) & order_volume['product_name'].isin(selected_products)]
    average_order_value = average_order_value[average_order_value['vendor_name'].isin(selected_vendors) & average_order_value['product_name'].isin(selected_products)]
    product_sales_data = product_sales_data[product_sales_data['vendor_name'].isin(selected_vendors) & product_sales_data['product_name'].isin(selected_products)]
    product_sales_data['product_sales'] = pd.to_numeric(product_sales_data['product_sales'], errors='coerce')
    product_portfolio = product_portfolio[product_portfolio['vendor_name'].isin(selected_vendors)]


    category_sales_vendor = category_sales_vendor[category_sales_vendor['vendor_name'].isin(selected_vendors)]
    category_sales_vendor['category_sales'] = pd.to_numeric(category_sales_vendor['category_sales'], errors='coerce')
    total_sales_vendor = total_sales_vendor[total_sales_vendor['vendor_name'].isin(selected_vendors)]
    total_sales_vendor['total_sales'] = pd.to_numeric(total_sales_vendor['total_sales'], errors='coerce')
    order_volume_vendor = order_volume_vendor[order_volume_vendor['vendor_name'].isin(selected_vendors)]
    average_order_value_vendor = average_order_value_vendor[average_order_value_vendor['vendor_name'].isin(selected_vendors)]
    product_sales_data_vendor = product_sales_data_vendor[product_sales_data_vendor['vendor_name'].isin(selected_vendors)]
    product_sales_data_vendor['product_sales'] = pd.to_numeric(product_sales_data_vendor['product_sales'], errors='coerce')
    # Streamlit dashboard

    # Summary section
    st.subheader("Summary")
    st.markdown(f"**Total Sales:** {total_sales['total_sales'].sum():,.2f}")
    st.markdown(f"**Total Orders:** {order_volume['order_count'].sum()}")
    st.markdown(f"**Average Order Value:** {average_order_value['average_order_value'].mean():,.2f}")

    top_products = most_sold_products.head(5)
    st.markdown("**Top 5 Most Sold Products:**")
    for i, row in top_products.iterrows():
        st.markdown(f"- {row['product_name']}: {row['total_sales']:,.2f}")

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
    st.markdown("""
    ### Total Sales
    This metric calculates the total revenue generated from sales over a specified period. It is calculated by summing the sales revenue for each product sold.
    """)
    sales_chart = alt.Chart(total_sales_vendor).mark_line(point=True).encode(
        x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
        y='total_sales:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'time_frame', 'total_sales']
    ).interactive().properties(title='Total Sales Over Time')
    st.altair_chart(sales_chart, use_container_width=True)
    
    total_sales['vendor_product'] = total_sales['vendor_name'] + ' - ' + total_sales['product_name']
    with st.expander("Expand to view Total Sales of each Product by each Vendor"):
        for vendor in selected_vendors:
            st.subheader(f'Total Sales for {vendor}')
            vendor_data = total_sales[total_sales['vendor_name'] == vendor]
            if not vendor_data.empty:
                vendor_data.loc[:, 'vendor_product'] = vendor_data['product_name']
                sales_chart_ = alt.Chart(vendor_data).mark_line(point=True).encode(
                    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                    y='total_sales:Q',
                    color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                    tooltip=['vendor_name', 'product_name', 'time_frame', 'total_sales']
                ).interactive().properties(
                    title=f'Total Sales Over Time for {vendor}'
                )
                
                st.altair_chart(sales_chart_, use_container_width=True)
            else:
                st.write(f"No data available for {vendor}")

    # Order Volume
    st.header('Order Volume by Vendor')
    st.markdown("""
    ### Order Volume
    This metric measures the total number of COMPLETED orders placed over a specified period. It indicates the quantity of products sold.
    """)
    volume_chart = alt.Chart(order_volume_vendor).mark_line(point=True).encode(
        x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
        y='order_count:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'time_frame', 'order_count']
    ).interactive().properties(title='Order Volume Over Time')
    st.altair_chart(volume_chart, use_container_width=True)

    order_volume['vendor_product'] = order_volume['vendor_name'] + ' - ' + order_volume['product_name']
    with st.expander("Expand to view Order Volume of each Product by each Vendor"):
        for vendor in selected_vendors:
            st.subheader(f'Order Volume for {vendor}')
            vendor_data = order_volume[order_volume['vendor_name'] == vendor]
            if not vendor_data.empty:
                vendor_data.loc[:, 'vendor_product'] = vendor_data['product_name']
                volume_chart_ = alt.Chart(vendor_data).mark_line(point=True).encode(
                    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                    y='order_count:Q',
                    color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                    tooltip=['vendor_name', 'product_name', 'time_frame', 'order_count']
                ).interactive().properties(
                    title=f'Order Volume Over Time for {vendor}'
                )
                st.altair_chart(volume_chart_, use_container_width=True)
            else:
                st.write(f"No data available for {vendor}")

    # Average Order Value
    st.header('Average Order Value by Vendor')
    st.markdown("""
    ### Average Order Value
    This metric calculates the average revenue per order. It is calculated by dividing the total sales revenue by the number of orders.
    """)
    aov_chart = alt.Chart(average_order_value_vendor).mark_line(point=True).encode(
        x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
        y='average_order_value:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'time_frame', 'average_order_value']
    ).interactive().properties(title='Average Order Value Over Time')

    st.altair_chart(aov_chart, use_container_width=True)
    average_order_value['vendor_product'] = average_order_value['vendor_name'] + ' - ' + average_order_value['product_name']
    with st.expander("Expand to view Average Order Value of each Product by each Vendor"):
        for vendor in selected_vendors:
            st.subheader(f'Average Order Value for {vendor}')
            vendor_data = average_order_value[average_order_value['vendor_name'] == vendor]
            if not vendor_data.empty:
                vendor_data.loc[:, 'vendor_product'] = vendor_data['product_name']
                aov_chart_ = alt.Chart(vendor_data).mark_line(point=True).encode(
                    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                    y='average_order_value:Q',
                    color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                    tooltip=['vendor_name', 'product_name', 'time_frame', 'average_order_value']
                ).interactive().properties(
                    title=f'Average Order Value Over Time for {vendor}'
                )
                st.altair_chart(aov_chart_, use_container_width=True)
                
            else:
                st.write(f"No data available for {vendor}")

    # Product Popularity
    st.header('Product Sales by Vendor')
    st.markdown("""
    ### Product Sales
    This metric calculates the overall popularity of each product based on the total sales amount within a specified time frame. It also considers only completed orders to ensure that the sales data is accurate.
    """)
    product_sales_data['vendor_product'] = product_sales_data['vendor_name'] + ' - ' + product_sales_data['product_name']
    with st.expander("Expand to view Product Sales of each Product by each Vendor"):
        for vendor in selected_vendors:
            st.subheader(f'Product Sales by {vendor}')
            vendor_data = product_sales_data[product_sales_data['vendor_name'] == vendor]
            if not vendor_data.empty:
                vendor_data.loc[:, 'vendor_product'] = vendor_data['product_name']
                popularity_chart = alt.Chart(vendor_data).mark_line(point=True).encode(
                    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                    y='product_sales:Q',
                    color=alt.Color('vendor_product:N', scale=alt.Scale(range=custom_colors)),
                    tooltip=['vendor_name', 'time_frame','product_name', 'product_sales']
                ).properties(
                    title=f'Product Sales Over Time for {vendor}'
                ).interactive()
            
                st.altair_chart(popularity_chart, use_container_width=True)
                
            else:
                st.write(f"No data available for {vendor}")
    st.header('Product Portfolio by Vendor')
    st.markdown("""
    ### Product Portfolio
    This metric shows how many unique products have been sold by each vendor, along with the total number of products they offer.
    """)

    # Create a line chart for sold products per vendor
    product_portfolio_chart = alt.Chart(product_portfolio).mark_line(point=True).encode(
            x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
            y='sold_product_count:Q',
            color='vendor_name_with_total:N',
            tooltip=['vendor_name_with_total', 'time_frame', 'sold_product_count', 'total_product_count']
        ).interactive()
   
    st.altair_chart(product_portfolio_chart, use_container_width=True, theme=None)
    # Category Sales
    st.header('Category Sales by Vendor')
    st.markdown("""
    ### Category Sales
    This metric calculates the total revenue generated from sales for each product category over a specified period.
    """)
    category_sales_chart = alt.Chart(category_sales).mark_line(point=True).encode(
            x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
            y='category_sales:Q',
            color='category_name_with_numbers:N',
            tooltip=['category_name_with_numbers:N', 'time_frame:T', 'category_sales:Q', 'num_completed_orders:Q', 'num_products:Q']
        ).properties(
            title='Category Sales Over Time', width =1000
        ).interactive()
   
    st.altair_chart(category_sales_chart, use_container_width=True)

    category_sales_vendor['vendor_category'] = category_sales_vendor['vendor_name'] + ' - ' + category_sales_vendor['category_name']
    with st.expander("Expand to view Category Sales of each Vendor"):
        for vendor in selected_vendors:
            st.subheader(f'Category Sales for {vendor}')
            vendor_data = category_sales_vendor[category_sales_vendor['vendor_name'] == vendor]
            if not vendor_data.empty:
                category_sales_chart_ = alt.Chart(vendor_data).mark_line(point=True).encode(
                    x=alt.X('time_frame:T', title='Date' if time_frame == 'Daily' else 'Time Frame'),
                    y='category_sales:Q',
                    color=alt.Color('category_name:N', scale=alt.Scale(range=custom_colors)),
                    tooltip=['vendor_category', 'time_frame','category_name', 'category_sales']
                ).interactive().properties(
                    title=f'Category Sales for {vendor}'
                )
              
                st.altair_chart(category_sales_chart_, use_container_width=True)
            else:
                st.write(f"No data available for {vendor}")
    
    #number of new vendors
    st.header('Number of New Vendors')
    st.markdown("""
    ### Number of New Vendors
    This metric tracks the number of vendors added to your system within the specified date range. 
    """)
    new_vendors = vendors[(vendors['created_at'].between(start_date, end_date))]
    new_vendor_count = new_vendors['vendor_id'].nunique()
    new_vendor_names = new_vendors['vendor_name'].tolist()


    # Display the names of new vendors
    if new_vendor_names:
        st.write("New Vendors Added:")
        st.write(", ".join(new_vendor_names))
        # Display the KPI
        st.metric(label="New Vendors", value=new_vendor_count)
    else:
        st.write("No new vendors added during the selected period.")
    
    # KPI Comparison
    st.header('KPI Comparison by Vendor')
    st.markdown("""
    ### KPI Comparison
    This section allows for comparing key performance indicators (KPIs) across different vendors to identify relative performance in various metrics.
    """)

    kpi_comparison_chart = alt.Chart(total_sales_vendor).mark_bar(size=5).encode(
        x='vendor_name:N',
        y='total_sales:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'total_sales']
    ).interactive().properties(title='Total Sales Comparison by Vendor')
    st.altair_chart(kpi_comparison_chart, use_container_width=True)

    # Comparison for Order Volume
    kpi_comparison_chart = alt.Chart(order_volume_vendor).mark_bar(size=5).encode(
        x='vendor_name:N',
        y='order_count:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'order_count']
    ).interactive().properties(title='Order Volume Comparison by Vendor')
    st.altair_chart(kpi_comparison_chart, use_container_width=True)

    # Comparison for Average Order Value
    kpi_comparison_chart = alt.Chart(average_order_value_vendor).mark_bar(size=5).encode(
        x='vendor_name:N',
        y='average_order_value:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'average_order_value']
    ).interactive().properties(title='Average Order Value Comparison by Vendor')
    st.altair_chart(kpi_comparison_chart, use_container_width=True)

    # Comparison for Product popularity
    kpi_comparison_chart = alt.Chart(product_sales_data_vendor).mark_bar(size=5).encode(
        x='vendor_name:N',
        y='product_sales:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'product_sales']
    ).interactive().properties(title='Product Popularity Comparison by Vendor')
    st.altair_chart(kpi_comparison_chart, use_container_width=True)
    # Comparison for Category sales
    kpi_comparison_chart = alt.Chart(category_sales_vendor).mark_bar(size=5).encode(
        x='vendor_name:N',
        y='category_sales:Q',
        color='vendor_name:N',
        tooltip=['vendor_name', 'category_sales']
    ).interactive().properties(title='Category Sales Comparison by Vendor')
    st.altair_chart(kpi_comparison_chart, use_container_width=True)

