"""
Lakehouse Monitor - Databricks App
Real-time operations, table metrics, and data quality dashboard for The PySpark Handbook.
"""

import os
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Lakehouse Monitor | PySpark Handbook",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("⚡ Lakehouse Operations & Quality Monitor")
st.markdown(
    """
    *Deployed natively via **Databricks Asset Bundles (DABs)**.*  
    Monitors Medallion tables, stream checkpoints, data quality expectations, and cluster metrics.
    """
)

# Sidebar filters
st.sidebar.header("Navigation & Configuration")
target_env = os.getenv("DATABRICKS_APP_PORT", "Local Dev")
st.sidebar.info(f"Target Environment: `{target_env}`")

catalog = st.sidebar.text_input("Unity Catalog", value="prod_catalog")
schema = st.sidebar.text_input("Target Schema", value="prod_lakehouse")

# Top KPI row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="Total Medallion Tables", value="18", delta="+2 today")
with col2:
    st.metric(label="Streaming Ingestion Latency", value="42 ms", delta="-8 ms")
with col3:
    st.metric(label="Data Quality Expectation Pass Rate", value="99.98%", delta="+0.02%")
with col4:
    st.metric(label="Active Serverless DBU/hr", value="2.4 DBU", delta="-0.6 DBU")

st.divider()

# Tabbed layout for deeper metrics
tab_tables, tab_dlt, tab_jobs = st.tabs(["📊 Medallion Tables", "🧱 DLT Pipeline Health", "⚡ Multi-Task Jobs"])

with tab_tables:
    st.subheader("Medallion Layer Table Inventory")
    sample_data = {
        "Layer": ["Bronze", "Bronze", "Silver", "Silver", "Gold", "Gold"],
        "Table Name": [
            "bronze_raw_events",
            "bronze_cdc_stream",
            "silver_validated_events",
            "silver_dim_customer_scd2",
            "gold_executive_daily_sales",
            "gold_fraud_monitoring_mart"
        ],
        "Format": ["Delta Lake", "Delta Lake", "Delta Lake", "Delta Lake", "Delta Lake", "Delta Lake"],
        "Clustering / Indexing": ["Liquid", "Partitioned", "Liquid", "Z-Order", "Liquid", "Liquid"],
        "Row Count": [12500000, 3400000, 12480000, 450000, 182500, 92000],
        "Last Optimized": ["10 min ago", "25 min ago", "12 min ago", "1 hr ago", "15 min ago", "15 min ago"],
        "Status": ["Healthy 🟢", "Healthy 🟢", "Healthy 🟢", "Healthy 🟢", "Healthy 🟢", "Healthy 🟢"]
    }
    df_tables = pd.DataFrame(sample_data)
    st.dataframe(df_tables, use_container_width=True)

with tab_dlt:
    st.subheader("Delta Live Tables (DLT) Data Expectations")
    expectations = {
        "Dataset": ["silver_validated_events", "silver_dim_customer_scd2", "silver_quarantine_dlq"],
        "Rule / Expectation": ["valid_timestamp: event_time IS NOT NULL", "valid_pk: customer_id IS NOT NULL", "quarantine_reason IS NOT NULL"],
        "Action": ["FAIL UPDATE", "DROP ROW", "TRACK METRIC"],
        "Passed Records": [12480000, 450000, 20000],
        "Failed Records": [0, 14, 0],
        "Pass Rate": ["100.0%", "99.997%", "100.0%"]
    }
    st.dataframe(pd.DataFrame(expectations), use_container_width=True)

with tab_jobs:
    st.subheader("Recent Automated DAB Orchestration Runs")
    st.success("✅ Master Pipeline [prod] run #1042 finished with status SUCCEEDED (Duration: 8m 42s)")
    st.code(
        """
        ├── 01_ingest_raw_files_autoloader         [SUCCEEDED - 1m 12s]
        ├── 02_cleanse_and_quarantine_records      [SUCCEEDED - 2m 04s]
        ├── 03_upsert_scd_type_2_dimensions        [SUCCEEDED - 1m 45s]
        ├── 04_mitigate_skew_and_join              [SUCCEEDED - 1m 55s]
        ├── 05_calculate_gold_analytics_marts      [SUCCEEDED - 1m 18s]
        └── 06_master_orchestration_audit          [SUCCEEDED - 0m 28s]
        """,
        language="text"
    )

st.caption("Powered by Databricks Apps, Unity Catalog, and Databricks Asset Bundles.")
