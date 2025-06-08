# NYC Yellow Taxi Dashboard
# Streamlit app to visualize EDA results

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt


# App title
st.title("NYC Yellow Taxi Dashboard - 2024")

# Load data
df_trips = pd.read_csv('../eda_results/total_trips_per_month_2024_clean.csv')
df_fare_tip = pd.read_csv('../eda_results/avg_fare_and_tip_per_month_2024_clean.csv')
df_payment_type = pd.read_csv('../eda_results/payment_type_trends_2024_clean.csv')

# Section 1: Total Trips per Month
st.header("Total Trips per Month")
fig1, ax1 = plt.subplots(figsize=(10,6))
ax1.plot(df_trips['month'], df_trips['total_trips'], marker='o')
ax1.set_title('Total NYC Yellow Taxi Trips per Month (2024)')
ax1.set_xlabel('Month')
ax1.set_ylabel('Total Trips')
ax1.grid(True)
plt.xticks(rotation=45)
plt.ylim(bottom=0)
st.pyplot(fig1)

# Section 2: Average Fare and Tip per Month
st.header("Average Fare and Tip per Month")
fig2, ax2 = plt.subplots(figsize=(10,6))
ax2.plot(df_fare_tip['month'], df_fare_tip['avg_fare'], marker='o', label='Average Fare')
ax2.plot(df_fare_tip['month'], df_fare_tip['avg_tip'], marker='o', label='Average Tip')
ax2.set_title('Average Fare and Tip per Month (2024)')
ax2.set_xlabel('Month')
ax2.set_ylabel('Amount ($)')
ax2.legend()
ax2.grid(True)
plt.xticks(rotation=45)
st.pyplot(fig2)

# Section 3: Payment Type Trends
st.header("Payment Type Distribution")
fig3, ax3 = plt.subplots(figsize=(8,6))
ax3.bar(df_payment_type['payment_type_label'], df_payment_type['total_trips'], color='skyblue')
ax3.set_title('Payment Type Distribution (2024)')
ax3.set_xlabel('Payment Type')
ax3.set_ylabel('Number of Trips')
ax3.grid(axis='y')
st.pyplot(fig3)

# Footer
st.markdown("---")
st.markdown("Data source: [NYC Yellow Taxi Trip Data 2024](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Project by [martinaspeciale](https://github.com/martinaspeciale)")
