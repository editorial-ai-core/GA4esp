import os
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, Dimension, Metric, OrderBy, Filter, FilterExpression, FilterExpressionList
)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG / UI
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Analytics Console", layout="wide")

st.markdown("""
<style>
.stButton > button {
    width: 100%;
    border-radius: 10px;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

def ui_error(msg: str):
    st.error(msg)

# ──────────────────────────────────────────────────────────────────────────────
# GA4 CLIENT
# ──────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

@st.cache_resource
def ga_client():
    sa = st.secrets.get("gcp_service_account")
    creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)

def default_property_id():
    return str(st.secrets.get("GA4_PROPERTY_ID", "")).strip()

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def clean_path(raw: str) -> str:
    if raw.startswith("http"):
        p = urlparse(raw)
        q = [(k, v) for k, v in parse_qsl(p.query) if not k.startswith("utm_")]
        return urlunparse(("", "", p.path, "", urlencode(q), ""))
    if not raw.startswith("/"):
        return "/" + raw
    return raw

# ──────────────────────────────────────────────────────────────────────────────
# GA4 QUERIES
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_urls(property_id, paths, start_date, end_date):
    client = ga_client()

    filters = [
        FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(value=p, match_type=Filter.StringFilter.MatchType.BEGINS_WITH)
            )
        ) for p in paths
    ]

    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="pagePath"), Dimension(name="pageTitle")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
            Metric(name="userEngagementDuration"),
        ],
        dimension_filter=FilterExpression(or_group=FilterExpressionList(expressions=filters)),
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        limit=100000,
    )

    resp = client.run_report(req)

    rows = []
    for r in resp.rows:
        v = int(float(r.metric_values[0].value))
        u = int(float(r.metric_values[1].value))
        e = float(r.metric_values[2].value)
        rows.append({
            "Path": r.dimension_values[0].value,
            "Title": r.dimension_values[1].value,
            "Views": v,
            "Users": u,
            "Avg Engagement (s)": round(e / max(u, 1), 1),
        })

    return pd.DataFrame(rows)

@st.cache_data(ttl=300)
def fetch_top(property_id, start_date, end_date, limit):
    client = ga_client()

    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),
            Dimension(name="screenClass"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
        limit=limit,
    )

    resp = client.run_report(req)

    return pd.DataFrame([{
        "Path": r.dimension_values[0].value,
        "Screen": r.dimension_values[1].value,
        "Views": int(float(r.metric_values[0].value)),
        "Users": int(float(r.metric_values[1].value)),
    } for r in resp.rows])

@st.cache_data(ttl=300)
def fetch_totals(property_id, start_date, end_date):
    client = ga_client()
    req = RunReportRequest(
        property=f"properties/{property_id}",
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="screenPageViews"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
    )
    r = client.run_report(req).rows[0].metric_values
    return int(r[0].value), int(r[1].value), int(r[2].value)

# ──────────────────────────────────────────────────────────────────────────────
# APP
# ──────────────────────────────────────────────────────────────────────────────
st.title("Analytics Console")

with st.sidebar:
    today = date.today()
    date_from = st.date_input("Date from", today - timedelta(days=30))
    date_to = st.date_input("Date to", today)
    property_id = st.text_input("GA4 Property ID", value=default_property_id())

tabs = st.tabs(["URL Analytics", "Top Materials", "Global Performance"])

# ──────────────────────────────────────────────────────────────────────────────
# TAB 1
# ──────────────────────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("URL Analytics")
    raw = st.text_area("URLs or paths (one per line)", height=200)

    if st.button("Collect", key="btn_urls"):
        if not raw.strip():
            ui_error("No URLs provided")
        else:
            paths = [clean_path(x.strip()) for x in raw.splitlines() if x.strip()]
            df = fetch_urls(property_id, paths, str(date_from), str(date_to))
            st.dataframe(df, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# TAB 2
# ──────────────────────────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Top Materials (GA4 parity)")

    limit = st.number_input("Limit", 1, 500, 10, key="limit_top")

    if st.button("Extract Top Content", key="btn_top"):
        df = fetch_top(property_id, str(date_from), str(date_to), int(limit))
        st.dataframe(df, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# TAB 3
# ──────────────────────────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("Global Performance")

    if st.button("Refresh Totals", key="btn_totals"):
        s, u, v = fetch_totals(property_id, str(date_from), str(date_to))
        c1, c2, c3 = st.columns(3)
        c1.metric("Sessions", f"{s:,}")
        c2.metric("Users", f"{u:,}")
        c3.metric("Views", f"{v:,}")
