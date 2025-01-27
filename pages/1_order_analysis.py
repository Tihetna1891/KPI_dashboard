import pandas as pd
import streamlit as st 
from db_pool import get_conn, release_conn
from statsmodels.formula.api import ols
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
import json
# Step 1: Fetch Data from SQL Query
@st.cache_data
def fetch_data():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            query = """
            WITH order_view AS (
                SELECT
                    gd.product_id,
                    pn."name" AS product_name,
                    pn.measuring_unit,
                    gc.id AS group_cart_id,
                    gd.id AS group_deal_id,
                    gd.group_price,
                    gd.max_group_member,
                    COALESCE(gd.lead_time, '24h') AS lead_time,
                    gc.user_id,
                    gc.quantity,
                    o.id AS order_id,
                    o.created_at::date AS ordered_at
                FROM
                    orders o
                JOIN groups_carts gc ON
                    gc.id = o.groups_carts_id
                    AND o.status = 'COMPLETED'
                    AND o.deleted_at IS NULL
                JOIN "groups" g ON g.id = gc.group_id
                JOIN group_deals gd ON gd.id = g.group_deals_id
                JOIN products p ON p.id = gd.product_id
                JOIN product_names pn ON pn.id = p.name_id
            ),
            time_series AS (
                SELECT t
                FROM generate_series(
                    date_trunc('DAY', '2024-08-01'::TIMESTAMP),
                    date_trunc('DAY', NOW()::TIMESTAMP),
                    INTERVAL '1 DAY'
                ) AS t
            ),
            active_customers AS (
                SELECT
                    ordered_at,
                    COUNT(DISTINCT user_id) AS active_customers,
                    COUNT(DISTINCT order_id) AS total_orders
                FROM order_view
                GROUP BY ordered_at
            ),
            active_ordering_customers AS (
                SELECT
                    ordered_at,
                    group_deal_id,
                    group_price,
                    max_group_member,
                    lead_time,
                    product_name,
                    measuring_unit,
                    COUNT(DISTINCT user_id) AS active_customer_ordered_product,
                    COUNT(DISTINCT order_id) AS product_order
                FROM order_view
                WHERE product_name = 'Red Onion B'
                GROUP BY
                    ordered_at, group_deal_id, product_name, measuring_unit, group_price, max_group_member, lead_time
            )
            SELECT
                ts.t AS sys_date,
                aoc.product_name,
                aoc.measuring_unit,
                aoc.group_deal_id,
                aoc.group_price,
                aoc.max_group_member,
                aoc.lead_time,
                ac.active_customers,
                aoc.active_customer_ordered_product,
                ac.total_orders,
                aoc.product_order
            FROM time_series ts
            LEFT JOIN active_ordering_customers aoc ON aoc.ordered_at = ts.t
            LEFT JOIN active_customers ac ON ac.ordered_at = ts.t;
            """
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=colnames)
            return df
        
    finally:
        if conn:
            release_conn(conn)

# Step 2: Load the CSV File
def load_config():
    with open("config.json", "r") as file:
        return json.load(file)

# Load the configuration
config = load_config()
# Retrieve the file path from the config
csv_path = config["files"]["updated_price_diff_csv"]


csv_data = pd.read_csv(csv_path)
df = fetch_data()

# Step 3: Convert Date Formats
df['sys_date'] = pd.to_datetime(df['sys_date'])
csv_data['Date'] = pd.to_datetime(csv_data['Date'])

# Step 1: Compute start_date and end_date for each group_deal_id
deal_duration = (
    df.groupby('group_deal_id')['sys_date']
    .agg(['min', 'max'])
    .reset_index()
    .rename(columns={'min': 'start_date', 'max': 'end_date'})
)

# Step 2: Calculate the duration of each deal in days
deal_duration['duration_days'] = (deal_duration['end_date'] - deal_duration['start_date']).dt.days


# Step 4: Perform the Join
merged_data = pd.merge(df, csv_data, left_on='sys_date', right_on='Date', how='outer')

# Merge the duration data back into the main analysis_data DataFrame
analysis_data = pd.merge(
    merged_data,
    deal_duration[['group_deal_id', 'duration_days']],
    on='group_deal_id',
    how='left'
)


# Step 1: Filter out rows where either 'group_price' or 'price' is NaN
valid_data = analysis_data.dropna(subset=['group_price', 'Price'])
# Step 1: Ensure 'group_price' and 'price' are of the same data type (convert to float)
valid_data['group_price'] = valid_data['group_price'].astype(float)
valid_data['Price'] = valid_data['Price'].astype(float)


# Step 2: Calculate the Percentage Difference
# Use the formula: ((price - group_price) / group_price) * 100
valid_data['percentage_diff'] = (( valid_data['group_price']-valid_data['Price'] ) / valid_data['Price'] ) * 100

csv_path = config["files"]["active_users_csv"]
active_user_data = pd.read_csv(csv_path)
# Replace 'None' values with np.nan
active_user_data.dropna(inplace=True)

active_user_data['sys_date'] = pd.to_datetime(active_user_data['sys_date'])

# Step 1.1: Drop Rows with Missing Values
active_user_data.dropna(subset=['active_user'], inplace=True)

# Step 1.2: Remove Outliers using IQR
Q1 = active_user_data['active_user'].quantile(0.25)
Q3 = active_user_data['active_user'].quantile(0.75)
IQR = Q3 - Q1

# Define outliers as data points outside of the range [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Keep only the data within the range
active_user_data = active_user_data[(active_user_data['active_user'] >= lower_bound) & 
                                    (active_user_data['active_user'] <= upper_bound)]
# Step 1: Extract Required Columns
First_analysis_data = valid_data[[
    'sys_date','group_deal_id','product_name', 'percentage_diff', 'active_customer_ordered_product', 
    'active_customers', 'max_group_member', 'lead_time', 'total_orders','duration_days'
]]

# Step 2: analysis_data is already loaded
# Merge the DataFrames
merged_data = pd.merge(First_analysis_data, active_user_data, on='sys_date', how='left')
# Step 1: Ensure `rounded_percentage_diff` is properly calculated for analysis_data
merged_data['rounded_percentage_diff'] = merged_data['percentage_diff'].round(1)
# Replace NaN values in the 'active_user' column with 0
merged_data['active_user'].fillna(0, inplace=True)

# Drop the unnecessary `percentage_diff` column after merging
merged_data = merged_data.drop(columns=['percentage_diff'])
st.subheader('Order Analysis from August 1 to October 17, 2024 for Red Onion B')
# General Description of Metrics
# Expandable section for General Description of Metrics
with st.expander("General Description of Metrics"):
    st.markdown("""
    ### General Description of Metrics

    The metrics presented in the visualizations are essential for analyzing business performance, particularly in terms of order fulfillment and customer engagement. Each metric is computed using aggregated data, providing a comprehensive overview of different dimensions of our operations:

    1. **Total Orders**: This metric represents the average number of orders received over specific time frames (e.g.,Delivery Time , discount categories). It is calculated by averaging the total number of orders across different segments of the data.

    2. **Active Customers**: This metric indicates the average number of unique customers who placed orders during the analysis period. It is computed by taking the mean of active customers for each segment, highlighting customer engagement and retention.

    3. **Active Users**: Similar to active customers, this metric reflects the average number of unique users interacting with our platform. It provides insight into the user base and overall platform utilization.

    4. **Data Point Count Calculation**
    - **Description**: The `duration_days` represents the number of individual data entries contributing to each aggregated segment of the analysis. It is calculated by grouping the data by `percentage_diff`, `max_group_member`, and `delivery_time`, and then counting the number of rows (entries) in each group.
    - **Details**: For each unique combination of `percentage_diff`, `max_group_member`, and `delivery_time`, the count of data points (`duration_days`) is determined, while also capturing the earliest (`start_date`) and latest (`end_date`) dates of the data entries. This metric helps to assess the reliability and density of the data within each segment, ensuring that insights are based on a significant number of observations.

    """)
    st.markdown("""
    ### Key Terms

    #### 1.Delivery Time  / Delivery Time 
    - **Description**:Delivery Time  refers to the total time taken from the moment an order is placed until it is delivered.

    #### 2. Discount Category
    - **Description**: 
        `discount_category` classifies the percentage difference between the Us(Chip Chip) and Market Place prices into distinct ranges. These categories help identify the extent of a discount or increase applied:

        - **High Discount (<-30%)**: A substantial price reduction of more than 30%.
        - **Medium Discounts 6 to 1**: Gradually decreasing discounts, ranging from -30% to 0%, with smaller intervals (e.g., -30% to -25%, -5% to 0%).
        - **No Discount (0%)**: Indicates that no price change has been applied.
        - **Increase 1 to 3**: Represents price increases, ranging from a small adjustment (0% to 10%) to a significant increase (above 20%).

    #### 3. Max Group Member
    - **Description**: Max group member indicates the maximum number of participants within a single customer group. 
    """)
    st.subheader('Row Data')
    st.write(merged_data)

# Step 2: Define Conditions and Create Classes
# print(f"Rows before dropping NaN in 'percentage_diff': {len(merged_data)}")
merged_data.dropna(subset=['rounded_percentage_diff'], inplace=True)

# Debug: Check how many rows were dropped
# print(f"Rows after dropping NaN in 'rounded_percentage_diff': {len(merged_data)}")
# 2.1 Categorize `percentage_diff`
def categorize_percentage_diff(diff):
    if diff < -30:
        return 'High Discount (<-30%)'
    elif -30 <= diff < -25:
        return 'Medium Discount 6 (-30% to -25%)'
    elif -25 <= diff < -20:
        return 'Medium Discount 5 (-25% to -20%)'
    elif -20 <= diff < -15:
        return 'Medium Discount 4 (-20% to -15%)'
    elif -15 <= diff < -10:
        return 'Medium Discount 3 (-15% to -10%)'
    elif -10 <= diff < -5:
        return 'Medium Discount 2 (-10% to -5%)'
    elif -5 <= diff < 0:
        return 'Medium Discount 1 (-5% to 0%)'
    elif diff == 0:
        return 'No Discount (0%)'
    elif 0 < diff <= 10:
        return 'Increase 1 (0% to 10%)'
    elif 10 < diff <= 20:
        return 'Increase 2 (10% to 20%)'
    elif diff > 20:
        return 'Increase 3 (>=20%)'


# Apply the categorization
merged_data['discount_category'] = merged_data['rounded_percentage_diff'].apply(categorize_percentage_diff)
# print("Unique Values for discount_category:", merged_data['discount_category'].unique())
# 2.2 Filter for specific `max_group_member` values and treat them as classes
merged_data = merged_data[merged_data['max_group_member'].isin(merged_data['max_group_member'].unique())]
# print(f"Rows after filtering by 'max_group_member': {len(merged_data)}")


# 2.3 Filter for specific `lead_time` values
merged_data['lead_time'] = merged_data['lead_time'].str.strip() 

merged_data = merged_data[merged_data['lead_time'].isin(merged_data['lead_time'].unique())]
# print(f"Rows after filtering by 'lead_time': {len(merged_data)}")
# Step 3: Group Data and Analyze Impact
grouped_analysis = merged_data.groupby(
    ['group_deal_id','discount_category', 'max_group_member', 'lead_time']
).agg({
    'active_customer_ordered_product': 'mean',
    'active_customers': 'mean',
    'active_user':'mean',
    'total_orders': 'mean',
    'duration_days': 'mean'
}).reset_index()
# print(f"Rows grouped_analysis: {len(grouped_analysis)}")


# print(grouped_analysis)
# Grouping and calculating mean `total_orders` by `max_group_member` subcategories
grouped_max_group_member = grouped_analysis.groupby('max_group_member').agg({
    'total_orders': 'mean',
    'active_customers': 'mean',
    'active_user': 'mean',
    'duration_days': 'mean'
}).reset_index()
grouped_max_group_member['duration_days'] = grouped_max_group_member['duration_days'].round(1).astype(int)
grouped_analysis['lead_time'] = grouped_analysis['lead_time'].str.replace('h', '').astype(int)
grouped_analysis['lead_time'] = pd.to_numeric(grouped_analysis['lead_time'], errors='coerce')

# Step 2: Get sorted unique lead_time values
sorted_lead_times = grouped_analysis['lead_time'].unique()
sorted_lead_times.sort()

grouped_lead_time = grouped_analysis.groupby('lead_time').agg({
    'total_orders': 'mean',
    'active_customers': 'mean',
    'active_user': 'mean',
    'duration_days': 'mean'
}).reset_index()
grouped_lead_time['duration_days'] = grouped_lead_time['duration_days'].round(1).astype(int)
# Grouping and calculating mean `total_orders` by `discount_category` subcategories
grouped_discount_category = grouped_analysis.groupby('discount_category').agg({
    'total_orders': 'mean',
    'active_customers': 'mean',
    'active_user': 'mean',
    'duration_days': 'mean'
}).reset_index()
# Round the duration_days mean
grouped_discount_category['duration_days'] = grouped_discount_category['duration_days'].round(1).astype(int)
# Sort the DataFrame by total_orders in ascending order
grouped_discount_category = grouped_discount_category.sort_values(by='total_orders', ascending=False)
# Plotting - Visualizing the effects of each subcategory

# 1. Max Group Member
st.markdown("""
#### 1. Average Total Orders, Active Customers, and Active Users by Max Group Member
- **Description**: This visualization examines how the size of customer groups (max group members) influences total orders and engagement metrics. It can help inform decisions regarding group promotions and targeting specific customer segments.
     - **Duration Days**: This metric represents the total time period (in days) that a specific group deal remains active and available for customers.
""")

# Create the plot
# Convert the 'max_group_member' bins into strings for better x-axis representation
grouped_max_group_member['max_group_member'] = grouped_max_group_member['max_group_member'].astype(str)

fig1, ax1 = plt.subplots(figsize=(10, 6))

# Plot total orders as a bar plot
bar_plot = sns.barplot(
    data=grouped_max_group_member, 
    x='max_group_member',  # Use numeric values for x-axis
    y='total_orders', 
    color='#7cb77b',
    ax=ax1,
    order=grouped_max_group_member['max_group_member']  # Ensure order is the same
)
# Adding data point counts inside bars with a description
for index, row in grouped_max_group_member.iterrows():
    # Get the height of the bar
    bar_height = bar_plot.patches[index].get_height()
    # Center the text vertically inside the bar
    ax1.text(index, bar_height / 2, f'duration: {row["duration_days"]}', ha='center', va='center', color='black',rotation=90)

ax1.set_title('Average Total Orders, Active Customers, and Active Users by Max Group Member')
ax1.set_xlabel('Max Group Member')
ax1.set_ylabel('Average Total Orders', color='green')
ax1.tick_params(axis='y', labelcolor='green')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)
# Create a secondary y-axis for active_customers and active_user
ax2 = ax1.twinx()

# Plot active_customers with a line plot
sns.lineplot(
    data=grouped_max_group_member, 
    x='max_group_member',  # Use numeric values for x-axis
    y='active_customers', 
    color='blue', 
    marker='o', 
    ax=ax2, 
    label='Active Customers',
    sort=False  # Prevent seaborn from sorting the x-values
)

# Plot active_user with another line plot
sns.lineplot(
    data=grouped_max_group_member, 
    x='max_group_member',  # Use numeric values for x-axis
    y='active_user', 
    color='red', 
    marker='o', 
    ax=ax2, 
    label='Active Users',
    sort=False  # Prevent seaborn from sorting the x-values
)

# Adjust the label and tick colors for the secondary axis
ax2.set_ylabel('Active Customers / Active Users', color='blue')
ax2.tick_params(axis='y', labelcolor='blue')

# Annotate the exact values on the markers for active_customers
for i in range(len(grouped_max_group_member)):
    ax2.text(
        x=i,  # Use index for x position
        y=grouped_max_group_member['active_customers'][i], 
        s=f"{grouped_max_group_member['active_customers'][i]:.0f}", 
        color='white', 
        ha='center', 
        va='bottom'
    )
    ax2.text(
        x=i,  # Use index for x position
        y=grouped_max_group_member['active_user'][i], 
        s=f"{grouped_max_group_member['active_user'][i]:.0f}", 
        color='black', 
        ha='center', 
        va='bottom'
    )

ax2.legend(loc='upper right',bbox_to_anchor=(1, 1.02))
# Show the plot in Streamlit
st.pyplot(fig1)

# 2.Delivery Time 

st.markdown("""
#### 2. Average Total Orders, Active Customers, and Active Users by Delivery Time 
-**Description**: This visualization shows the relationship betweenDelivery Time  and the average number of orders. Understanding howDelivery Time  affects order volume can help optimize delivery processes and enhance customer satisfaction.
     -**Duration Days**: This metric represents the total time period (in days) that a specific group deal remains active and available for customers.
  """)
# Create the figure and primary axis
# Convert the 'lead_time' bins into strings for better x-axis representation
grouped_lead_time['lead_time'] = grouped_lead_time['lead_time'].astype(str)

# Create the plot
fig2, ax1 = plt.subplots(figsize=(10, 6))

# Plot total orders as a bar plot
bar_plot=sns.barplot(
    data=grouped_lead_time, 
    x='lead_time', 
    y='total_orders', 
    # palette='Greens_d', 
    color='#7cb77b',
    ax=ax1
)

# Adding data point counts inside bars with a description
for index, row in grouped_lead_time.iterrows():
    # Get the height of the bar
    bar_height = bar_plot.patches[index].get_height()
    # Center the text vertically inside the bar
    ax1.text(index, bar_height / 2, f'duration: {row["duration_days"]}', ha='center', va='center', color='black')

ax1.set_title('Average Total Orders, Active Customers, and Active Users byDelivery Time ')
ax1.set_xlabel(' Delivery Time')
ax1.set_ylabel('Average Total Orders', color='green')
ax1.tick_params(axis='y', labelcolor='green')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Create a secondary y-axis for active_customers and active_user
ax2 = ax1.twinx()

# Plot active_customers with a line plot
sns.lineplot(
    data=grouped_lead_time, 
    x='lead_time', 
    y='active_customers', 
    color='blue', 
    marker='o', 
    ax=ax2, 
    label='Active Customers'
)

# Plot active_user with another line plot
sns.lineplot(
    data=grouped_lead_time, 
    x='lead_time', 
    y='active_user', 
    color='red', 
    marker='o', 
    ax=ax2, 
    label='Active Users'
)

# Adjust the label and tick colors for the secondary axis
ax2.set_ylabel('Active Customers / Active Users', color='blue')
ax2.tick_params(axis='y', labelcolor='blue')

# Annotate the exact values on the markers for active_customers
for i in range(len(grouped_lead_time)):
    ax2.text(
        x=grouped_lead_time['lead_time'][i], 
        y=grouped_lead_time['active_customers'][i], 
        s=f"{grouped_lead_time['active_customers'][i]:.0f}", 
        color='white', 
        ha='center', 
        va='bottom'
    )
    ax2.text(
        x=grouped_lead_time['lead_time'][i], 
        y=grouped_lead_time['active_user'][i], 
        s=f"{grouped_lead_time['active_user'][i]:.0f}", 
        color='black', 
        ha='center', 
        va='bottom'
    )

ax2.legend(loc='upper right',bbox_to_anchor=(1, 1.02))

# Show the plot in Streamlit
st.pyplot(fig2)

# 3. Discount Category
st.markdown("""         
#### 3. Average Total Orders, Active Customers, and Active Users by Discount Category
- **Description**: This visualization compares the average total orders against active customers and active users for different discount categories. It highlights the effectiveness of discount strategies in attracting customers and driving orders, enabling targeted marketing efforts.
           --**Duration Days**: This metric represents the total time period (in days) that a specific group deal remains active and available for customers. """)
# Create the plot
grouped_discount_category['discount_category'] = grouped_discount_category['discount_category'].astype(str)

fig3, ax1 = plt.subplots(figsize=(10, 6))

# Plot total orders as a bar plot
bar_plot=sns.barplot(
    data=grouped_discount_category, 
    x='discount_category', 
    y='total_orders', 
    color='#7cb77b',
    # palette='Greens_d', 
    ax=ax1,
    order=grouped_discount_category['discount_category']  # Ensure order is the same
)
# Adding data point counts inside bars with a description
for index, row in grouped_discount_category.iterrows():
    # Get the height of the bar
    bar_height = bar_plot.patches[index].get_height()
    # Center the text vertically inside the bar
    ax1.text(index, bar_height / 2, f'duration: {row["duration_days"]}', ha='center', va='center', color='black',rotation=90)

ax1.set_title('Average Total Orders, Active Customers, and Active Users by Discount Category')
ax1.set_xlabel('Discount Category')
ax1.set_ylabel('Average Total Orders', color='green')
ax1.tick_params(axis='y', labelcolor='green')

# Rotate x-axis labels for better readability

ax1.set_xticklabels(grouped_discount_category['discount_category'], rotation=45, ha='right')


# Create a secondary y-axis for active_customers and active_user
ax2 = ax1.twinx()

# Plot active_customers with a line plot
sns.lineplot(
    data=grouped_discount_category, 
    x='discount_category', 
    y='active_customers', 
    color='blue', 
    marker='o', 
    ax=ax2, 
    label='Active Customers',
    sort=False  # Prevent seaborn from sorting the x-values
)

# Plot active_user with another line plot
sns.lineplot(
    data=grouped_discount_category, 
    x='discount_category', 
    y='active_user', 
    color='red', 
    marker='o', 
    ax=ax2, 
    label='Active Users',
    sort=False  # Prevent seaborn from sorting the x-values
)

# Adjust the label and tick colors for the secondary axis
ax2.set_ylabel('Active Customers / Active Users', color='blue')
ax2.tick_params(axis='y', labelcolor='blue')

# Annotate the exact values on the markers for active_customers
for i in range(len(grouped_discount_category)):
    ax2.text(
        x=i,  # Use index for x position
        y=grouped_discount_category['active_customers'][i], 
        s=f"{grouped_discount_category['active_customers'][i]:.0f}", 
        color='white', 
        ha='center', 
        va='bottom'
    )
    ax2.text(
        x=i,  # Use index for x position
        y=grouped_discount_category['active_user'][i], 
        s=f"{grouped_discount_category['active_user'][i]:.0f}", 
        color='black', 
        ha='center', 
        va='bottom'
    )

# Combine legends into one, making it clearer which line is which

ax2.legend(loc='upper right',bbox_to_anchor=(1, 1.02))
# Show the plot in Streamlit
st.pyplot(fig3)

# Create a copy of the dataframe
df_encoded = grouped_analysis.copy()
# Encode the categorical features
label_encoders = {}
for column in ['lead_time', 'discount_category']:
    label_encoders[column] = LabelEncoder()
    df_encoded[column] = label_encoders[column].fit_transform(df_encoded[column])
# Create a mapping DataFrame for lead_time
lead_time_mapping = pd.DataFrame({
    'OriginalDelivery Time ': label_encoders['lead_time'].inverse_transform(range(len(label_encoders['lead_time'].classes_))),
    'Encoded Value': range(len(label_encoders['lead_time'].classes_))
})

# Create a mapping DataFrame for discount_category
discount_category_mapping = pd.DataFrame({
    'Original Discount Category': label_encoders['discount_category'].inverse_transform(range(len(label_encoders['discount_category'].classes_))),
    'Encoded Value': range(len(label_encoders['discount_category'].classes_))
})
st.markdown("""
            #### 4. Heatmap of Average Total Orders by Max Group Member,Delivery Time , and Discount Category
- **Description**: This heatmap provides a multi-dimensional view of total orders, allowing for an analysis of how various factors interact. It displays averages for total orders based on combinations of max group members,Delivery Time , and discount categories, facilitating quick identification of trends and anomalies.

            """)

# Create two columns for side-by-side display
col1, col2 = st.columns(2)

# DisplayDelivery Time  mapping in the first column
with col1:
    st.write("### Delivery Time  Encoding")
    st.dataframe(lead_time_mapping)

# Display discount category mapping in the second column
with col2:
    st.write("### Discount Category Encoding")
    st.dataframe(discount_category_mapping)
# Correlation matrix
correlation_matrix = df_encoded[['max_group_member', 'lead_time', 'discount_category', 'total_orders']].corr()

# Encode categorical variables if not already done
df_encoded['lead_time'] = df_encoded['lead_time'].astype('category')
df_encoded['discount_category'] = df_encoded['discount_category'].astype('category')

# Fit a regression model
model = ols('total_orders ~ C(max_group_member) + C(lead_time) + C(discount_category)', data=df_encoded).fit()

# Print the summary of the regression
# print(model.summary())

# Pivot the data for heatmap
heatmap_data = df_encoded.groupby(['max_group_member', 'discount_category', 'lead_time']).agg({'total_orders': 'mean'}).reset_index()
heatmap_pivot = heatmap_data.pivot_table(values='total_orders', index=['max_group_member', 'lead_time'], columns='discount_category')

fig6 = plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_pivot, cmap='coolwarm', annot=True, fmt='.1f')
plt.title('Average Total Orders Heatmap by Max Group Member,Delivery Time , and Discount Category')
plt.xlabel('Discount Category')
plt.ylabel('Max Group Member andDelivery Time ')
st.pyplot(fig6)

fig7 = plt.figure(figsize=(12, 6))
sns.barplot(data=df_encoded, x='max_group_member', y='total_orders', hue='discount_category', palette='Set2', ci=None)
plt.title('Total Orders by Max Group Member and Discount Category')
plt.xlabel('Max Group Member')
plt.ylabel('Total Orders')
plt.xticks(rotation=45)
plt.legend(title='Discount Category')
st.pyplot(fig7)

# Create a combined bar plot with color indicating lead_time
fig = plt.figure(figsize=(12, 6))
sns.barplot(data=df_encoded, x='max_group_member', y='total_orders',
            hue='lead_time', palette='Set2', errorbar=None)
plt.title('Total Orders by Max Group Member and Delivery Time ')
plt.xlabel('Max Group Member')
plt.ylabel('Total Orders')
plt.xticks(rotation=45)
plt.legend(title=' Delivery Time')
st.pyplot(fig)


