# -*- coding: utf-8 -*-

import streamlit as st

from src.data_loader import cargar_datos
from src.ui.theme import configure_page, apply_plotly_theme, inject_global_styles
from src.ui.sidebar import render_sidebar_filters, render_sidebar_export
from src.ui.header import render_header
from src.ui.tabs import render_tabs
from src.ui.reporting import render_report_section
from src.ui.chat import render_chat_section


# =============================================================================
# 1. Configuración de la página
# =============================================================================
configure_page()

# =============================================================================
# 1.1 Estilo global y tema visual (Plotly + Streamlit)
# =============================================================================
apply_plotly_theme()
inject_global_styles()


# =============================================================================
# 2. Carga de datos centralizada
# =============================================================================
try:
    df_dss, health_scores, metricas_calidad = cargar_datos()
except Exception as e:
    st.error(f"❌ Error al cargar los datos: {e}")
    st.stop()


# =============================================================================
# 3. Sidebar – Filtros globales y exportación
# =============================================================================
df_filtrado = render_sidebar_filters(df_dss)
render_sidebar_export(df_filtrado)


# =============================================================================
# 4. Encabezado principal
# =============================================================================
render_header(df_filtrado, health_scores)


# =============================================================================
# 5. Navegación por pestañas
# =============================================================================
render_tabs(df_filtrado, health_scores, metricas_calidad)


# =============================================================================
# 6. Generación de Reporte PDF (FIX DEFINITIVO STREAMLIT STATE)
# =============================================================================
render_report_section(df_filtrado, health_scores, metricas_calidad)


# =============================================================================
# 7. Asistente IA (Groq)
# =============================================================================
render_chat_section(df_filtrado, health_scores)


# =============================================================================
# Footer
# =============================================================================
st.sidebar.markdown("---")
st.sidebar.caption("© 2026 TechLogistics S.A.S – Dashboard de Auditoría Técnica")
