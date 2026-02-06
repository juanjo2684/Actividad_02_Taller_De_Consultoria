# -*- coding: utf-8 -*-
import sys
from datetime import datetime

import streamlit as st

from src.reportes import generar_reporte_ejecutivo_pdf


def _init_report_state() -> None:
    if "pdf_reporte" not in st.session_state:
        st.session_state.pdf_reporte = None


def render_report_section(df_filtrado, health_scores, metricas_calidad) -> None:
    st.sidebar.markdown("---")
    st.sidebar.subheader("📄 Reporte Ejecutivo PDF")

    _init_report_state()

    if st.sidebar.button("🛠️ Preparar Reporte"):
        try:
            with st.spinner("🔄 Generando reporte ejecutivo..."):
                st.session_state.pdf_reporte = generar_reporte_ejecutivo_pdf(
                    df_filtrado=df_filtrado,
                    health_scores=health_scores,
                    metricas_calidad=metricas_calidad,
                )

            st.sidebar.success("✅ Reporte listo para descargar")

        except Exception:
            st.sidebar.error("❌ Error al generar el reporte PDF")
            import traceback
            print(traceback.format_exc(), file=sys.stderr)

    if st.session_state.pdf_reporte is not None:
        st.sidebar.download_button(
            label="⬇️ Descargar Reporte PDF",
            data=st.session_state.pdf_reporte,
            file_name=f"Reporte_Ejecutivo_TechLogistics_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf"
        )
