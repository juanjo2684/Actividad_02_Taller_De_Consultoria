# -*- coding: utf-8 -*-
import streamlit as st
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go


def configure_page() -> None:
    st.set_page_config(
        page_title="TechLogistics DSS - Dashboard Ejecutivo",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )


def apply_plotly_theme() -> None:
    pio.templates["techlogistics"] = go.layout.Template(
        layout=go.Layout(
            font=dict(family="Inter, Segoe UI, sans-serif", size=12, color="#1f2a44"),
            paper_bgcolor="#f6f8fb",
            plot_bgcolor="#ffffff",
            colorway=["#1f4e78", "#2e75b6", "#00a1d6", "#7f3fbf", "#f39c12", "#e74c3c"],
            xaxis=dict(
                gridcolor="#e9eef5",
                zerolinecolor="#d9e2ef",
                showline=True,
                linecolor="#d9e2ef",
                tickcolor="#d9e2ef"
            ),
            yaxis=dict(
                gridcolor="#e9eef5",
                zerolinecolor="#d9e2ef",
                showline=True,
                linecolor="#d9e2ef",
                tickcolor="#d9e2ef"
            ),
            legend=dict(orientation="h", y=-0.2)
        )
    )
    pio.templates.default = "techlogistics"
    px.defaults.template = "techlogistics"
    px.defaults.color_discrete_sequence = [
        "#1f4e78", "#2e75b6", "#00a1d6", "#7f3fbf", "#f39c12", "#e74c3c"
    ]
    px.defaults.color_continuous_scale = "Blues"


def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        :root {
            --primary: #1f4e78;
            --secondary: #00a1d6;
            --accent: #7f3fbf;
            --background: #f6f8fb;
            --card: #ffffff;
            --text: #1f2a44;
            --muted: #6b7c93;
            --border: #e3eaf3;
        }
        html, body, [class*="css"] {font-family: 'Inter', Segoe UI, sans-serif;}
        .stApp {background: var(--background); color: var(--text);}
        .block-container {padding-top: 1.25rem; padding-bottom: 2rem; max-width: 1400px;}
        h1, h2, h3 {letter-spacing: 0.2px; color: var(--text);}
        h1 {font-weight: 700;}
        h2, h3 {font-weight: 600;}

        /* Sidebar */
        section[data-testid="stSidebar"] {background: #ffffff; border-right: 1px solid var(--border);}
        section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 {color: var(--text);}
        section[data-testid="stSidebar"] .stButton > button {width: 100%;}

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {gap: 0.25rem;}
        .stTabs [data-baseweb="tab"] {
            font-size: 0.92rem;
            padding: 0.35rem 0.85rem;
            border-radius: 999px;
            border: 1px solid var(--border);
            background: #ffffff;
            color: var(--muted);
        }
        .stTabs [aria-selected="true"] {
            background: var(--primary);
            color: #ffffff;
            border-color: var(--primary);
            box-shadow: 0 6px 16px rgba(31, 78, 120, 0.18);
        }

        /* Metric cards */
        .stMetric {
            background: var(--card);
            border: 1px solid var(--border);
            padding: 16px;
            border-radius: 14px;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.06);
        }
        .stMetric label {color: var(--muted); font-weight: 600;}
        .stMetric [data-testid="stMetricValue"] {color: var(--text);}

        /* Expanders */
        .stExpander {
            border: 1px solid var(--border);
            border-radius: 12px;
            background: #ffffff;
        }

        /* Dataframes */
        .stDataFrame {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }

        /* Buttons */
        .stButton > button {
            border-radius: 10px;
            font-weight: 600;
            border: 1px solid var(--primary);
            color: #ffffff;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
        }
        .stDownloadButton > button {
            border-radius: 10px;
            font-weight: 600;
            border: 1px solid var(--primary);
            background: #ffffff;
            color: var(--primary);
        }

        /* KPI percentage emphasis – always neutral blue */
        .kpi-percentage,
        .kpi-percentage.risk {
            margin-top: 0.35rem;
            padding: 0.35rem 0.6rem;
            display: inline-block;
            border-radius: 999px;
            background: rgba(0, 161, 214, 0.12);
            color: var(--primary);
            font-weight: 700;
            font-size: 1.15rem;
            letter-spacing: 0.2px;
        }

        /* Force all st.metric deltas to neutral blue & hide arrow icons */
        [data-testid="stMetricDelta"] {
            color: #2e75b6 !important;
        }
        [data-testid="stMetricDelta"] svg {
            display: none !important;
        }

        /* Enhanced table / dataframe styling */
        .stDataFrame table,
        .stTable table {
            border-collapse: separate;
            border-spacing: 0;
            width: 100%;
            font-size: 0.88rem;
        }
        .stDataFrame thead th,
        .stTable thead th {
            background: #1f4e78 !important;
            color: #ffffff !important;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.3px;
            padding: 10px 12px !important;
            border-bottom: 2px solid #163d5c !important;
        }
        .stDataFrame tbody td,
        .stTable tbody td {
            padding: 8px 12px !important;
            border-bottom: 1px solid #e9eef5 !important;
        }
        .stDataFrame tbody tr:nth-child(even),
        .stTable tbody tr:nth-child(even) {
            background: #f6f8fb;
        }
        .stDataFrame tbody tr:hover,
        .stTable tbody tr:hover {
            background: rgba(0, 161, 214, 0.08);
        }
        </style>
        """,
        unsafe_allow_html=True
    )
