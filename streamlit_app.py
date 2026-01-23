import os
from pathlib import Path
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import numpy as np
import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, Dimension, Metric, Filter, FilterExpression,
    FilterExpressionList, OrderBy
)

# ──────────────────────────────────────────────────────────────────────────────
# UI / Styling
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Analytics Console", layout="wide")

st.markdown("""
<style>
.main { background-color: #f8fafc; }
.stButton>button {
  width: 100%;
  border-radius: 12px;
  font-weight: 700;
  background-color: #0f172a;
  color: white;
  border: none;
  padding: 0.6rem;
}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def fail_ui(msg: str):
    st.error(msg)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# GA4 client
# ──────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

@st.cache_resource
def ga_client() -> BetaAnalyticsDataClient:
    sa = st.secrets.get("gcp_service_account")
    if not sa:
        fail_ui("Missing Streamlit Secret: gcp_service_account")
    creds = service_account.Credentials.from_service_account_info(dict(sa), scopes=SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)

def default_property_id() -> str:
    pid = str(st.secrets.get("GA4_PROPERTY_ID", "")).strip()
    if not pid:
        fail_ui("Missing Streamlit Secret: GA4_PROPERTY_ID")
    return pid

# ──────────────────────────────────────────────────────────────────────────────
# GA4 Queries
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_top_materials_cached(
    property_id: str,
    start_date: str,
    end_date: str,
    limit: int,
) -> pd.DataFrame:
    """
    1:1 соответствует GA4:
    Dimension = 'Путь к странице и класс экрана'
    Metric    = screenPageViews
    """
    client = ga_client()

    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),
            Dimension(name="pageTitle"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
            Metric(name="userEngagementDuration"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        order_bys=[
            OrderBy(
                metric=OrderBy.MetricOrderBy(
                    metric_name="screenPageViews"
                ),
                desc=True,
            )
        ],
        limit=int(limit),
    )

    resp = client.run_report(req)

    rows = []
    for r in resp.rows:
        views = int(float(r.metric_values[0].value or 0))
        users = int(float(r.metric_values[1].value or 0))
        eng = float(r.metric_values[2].value or 0)

        rows.append({
            "Path": r.dimension_values[0].value,
            "Title": r.dimension_values[1].value,
            "Views": views,
            "Unique Users": users,
            "Avg Engagement Time (s)": round(eng / max(users, 1), 1),
        })

    return pd.DataFrame(rows)

@st.cache_data(ttl=300)
def fetch_site_totals_cached(property_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    client = ga_client()
    req = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="screenPageViews"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=1,
    )
    resp = client.run_report(req)
    row = resp.rows[0].metric_values if resp.rows else []
    return pd.DataFrame([{
        "sessions": int(row[0].value) if row else 0,
        "totalUsers": int(row[1].value) if row else 0,
        "screenPageViews": int(row[2].value) if row else 0,
    }])

# ──────────────────────────────────────────────────────────────────────────────
# App
# ──────────────────────────────────────────────────────────────────────────────
st.title("Analytics Console")
st.markdown("GA4 reporting with exact parity to standard GA4 reports.")

with st.sidebar:
    today = date.today()
    date_from = st.date_input("Date From", value=today - timedelta(days=30))
    date_to = st.date_input("Date To", value=today)
    property_id = st.text_input("GA4 Property ID", value=default_property_id())

tab1, tab2, tab3 = st.tabs(["Top Materials", "Global Performance", "—"])

# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 — Top Materials
# ──────────────────────────────────────────────────────────────────────────────
with tab1:
    st.subheader("High-Performance Content (Top by Page Views)")

    limit = st.number_input(
        "Limit",
        min_value=1,
        max_value=500,
        value=10,
        step=1,
    )

    if st.button("Extract Top Content"):
        if date_from > date_to:
            fail_ui("Date From must be <= Date To.")
        pid = property_id.strip()
        if not pid:
            fail_ui("GA4 Property ID is empty.")

        with st.spinner("Fetching GA4…"):
            df_top = fetch_top_materials_cached(
                property_id=pid,
                start_date=str(date_from),
                end_date=str(date_to),
                limit=int(limit),
            )

        if df_top.empty:
            st.info("No data returned for this period.")
        else:
            st.dataframe(df_top, use_container_width=True, hide_index=True)
            st.download_button(
                "Export CSV",
                df_top.to_csv(index=False).encode("utf-8"),
                "ga4_top_materials.csv",
                "text/csv",
            )

# ──────────────────────────────────────────────────────────────────────────────
# TAB 2 — Global Performance
# ──────────────────────────────────────────────────────────────────────────────
with tab2:
    st.subheader("Global Site Summary")

    if st.button("Refresh Site Totals"):
        if date_from > date_to:
            fail_ui("Date From must be <= Date To.")
        pid = property_id.strip()
        if not pid:
            fail_ui("GA4 Property ID is empty.")

        totals = fetch_site_totals_cached(pid, str(date_from), str(date_to))

        c1, c2, c3 = st.columns(3)
        c1.metric("Sessions", f"{int(totals.loc[0, 'sessions']):,}")
        c2.metric("Unique Users", f"{int(totals.loc[0, 'totalUsers']):,}")
        c3.metric("Page Views", f"{int(totals.loc[0, 'screenPageViews']):,}")
