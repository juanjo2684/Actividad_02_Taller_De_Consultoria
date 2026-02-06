# -*- coding: utf-8 -*-

import streamlit as st

from src.data_loader import cargar_datos, render_file_upload_section
from src.ui.theme import configure_page, apply_plotly_theme, inject_global_styles
from src.ui.sidebar import render_sidebar_filters, render_sidebar_export
from src.ui.header import render_header
from src.ui.tabs import render_tabs
from src.ui.reporting import render_report_section
from src.ui.chat import render_chat_sidebar_config, render_chat_panel


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
# 2. Carga de archivos (upload con defaults)
# =============================================================================
ruta_inv, ruta_feed, ruta_trans = render_file_upload_section()

try:
    df_dss, health_scores, metricas_calidad = cargar_datos(
        ruta_inv, ruta_feed, ruta_trans
    )
except Exception as e:
    st.error(f"❌ Error al cargar los datos: {e}")
    st.stop()


# =============================================================================
# 3. Sidebar – Filtros globales y exportación
# =============================================================================
df_filtrado = render_sidebar_filters(df_dss)
df_filtrado.rename(columns={"Bodega_Origen_x":"Bodega_Origen"}, inplace=True)
render_sidebar_export(df_filtrado)


# =============================================================================
# 4. Sidebar – Configuración del chat IA
# =============================================================================
render_chat_sidebar_config()
render_report_section(df_filtrado, health_scores, metricas_calidad)


# =============================================================================
# 5. Layout principal: contenido (izq) + chat (der)
# =============================================================================
col_main, col_chat = st.columns([3, 1])

with col_main:
    # ── Encabezado principal
    render_header(df_filtrado, health_scores)

    # ── Navegación por pestañas
    render_tabs(df_filtrado, health_scores, metricas_calidad)

with col_chat:
    render_chat_panel(df_filtrado, health_scores)


# =============================================================================
# Footer
# =============================================================================
st.sidebar.markdown("---")
st.sidebar.caption("© 2026 TechLogistics S.A.S – Dashboard de Auditoría Técnica")
