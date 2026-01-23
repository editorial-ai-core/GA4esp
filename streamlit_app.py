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

INVISIBLE = ("\ufeff", "\u200b", "\u2060", "\u00a0")

def clean_line(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    for ch in INVISIBLE:
        s = s.replace(ch, "")
    return s.strip()

def strip_utm_and_fragment(raw_url: str) -> str:
    p = urlparse(raw_url)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), ""))

def normalize_input(raw: str) -> str:
    s = clean_line(raw)
    if not s:
        return ""
    if s.startswith("http"):
        s = strip_utm_and_fragment(s)
        return urlparse(s).path or "/"
    if not s.startswith("/"):
        s = "/" + s
    return s

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
# GA4 — URL Analytics (TAB 1)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_by_paths_cached(property_id: str, paths: tuple, start_date: str, end_date: str) -> pd.DataFrame:
    client = ga_client()

    exprs = [
        FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    value=p,
                    match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                    case_sensitive=False,
                )
            )
        )
        for p in paths
    ]

    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="pagePath"), Dimension(name="pageTitle")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
            Metric(name="userEngagementDuration"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(expressions=exprs)
        ),
        limit=100000,
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

# ──────────────────────────────────────────────────────────────────────────────
# GA4 — Top Materials (TAB 2) — 1:1 GA4 UI
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_top_materials_cached(property_id: str, start_date: str, end_date: str, limit: int) -> pd.DataFrame:
    """
    Полное соответствие GA4:
    Dimension = 'Путь к странице и класс экрана'
    """
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
            Metric(name="userEngagementDuration"),
        ],
        date_ranges=[{"start_date": start_date, "end_date": end_date}],
        order_bys=[
            OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"),
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
            "Screen Class": r.dimension_values[1].value,
            "Views": views,
            "Unique Users": users,
            "Avg Engagement Time (s)": round(eng / max(users, 1), 1),
        })

    return pd.DataFrame(rows)

# ──────────────────────────────────────────────────────────────────────────────
# GA4 — Global Totals (TAB 3)
# ──────────────────────────────────────────────────────────────────────────────
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

with st.sidebar:
    today = date.today()
    date_from = st.date_input("Date From", value=today - timedelta(days=30))
    date_to = st.date_input("Date To", value=today)
    property_id = st.text_input("GA4 Property ID", value=default_property_id())

tab1, tab2, tab3 = st.tabs(["URL Analytics", "Top Materials", "Global Performance"])

# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 — URL Analytics
# ───────
