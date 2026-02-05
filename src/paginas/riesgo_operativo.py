# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime


# =============================================================================
# FUNCIÓN PURA: construcción de la figura (NO usa Streamlit)
# =============================================================================
def construir_fig_riesgo_operativo(df_filtrado: pd.DataFrame):

    df = df_filtrado.copy()

    columnas_requeridas = {
        "Ultima_Revision",
        "Bodega_Origen",
        "Ticket_Soporte",
        "ingreso_total",
        "NPS_Numerico"
    }

    if not columnas_requeridas.issubset(df.columns):
        fig = px.scatter(title="Impacto del Descuido Operativo por Bodega")
        fig.add_annotation(
            text="Columnas requeridas no disponibles para el análisis de riesgo",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False
        )
        return fig

    if df.empty:
        fig = px.scatter(title="Impacto del Descuido Operativo por Bodega")
        fig.add_annotation(
            text="No hay datos para los filtros actuales",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False
        )
        return fig

    df["Ultima_Revision"] = pd.to_datetime(df["Ultima_Revision"], errors="coerce")
    fecha_referencia = df["Ultima_Revision"].max()
    df["dias_sin_revision"] = (fecha_referencia - df["Ultima_Revision"]).dt.days

    df = df.dropna(subset=["dias_sin_revision"])

    if df.empty:
        fig = px.scatter(title="Impacto del Descuido Operativo por Bodega")
        fig.add_annotation(
            text="No se pudo calcular la antigüedad de revisión",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False
        )
        return fig

    # Agregación por bodega
    df_bodega = (
        df
        .groupby("Bodega_Origen")
        .agg(
            dias_sin_revision=("dias_sin_revision", "mean"),
            Ticket_Soporte=("Ticket_Soporte", lambda x: x.mean() * 100),
            ingreso_total=("ingreso_total", "sum"),
            NPS_Numerico=("NPS_Numerico", "mean")
        )
        .reset_index()
    )

    fig = px.scatter(
        df_bodega,
        x="dias_sin_revision",
        y="Ticket_Soporte",
        size="ingreso_total",
        color="Ticket_Soporte",
        hover_name="Bodega_Origen",
        hover_data={"NPS_Numerico": ":.2f"},
        color_continuous_scale="Reds",
        labels={
            "dias_sin_revision": "Días desde Última Revisión",
            "Ticket_Soporte": "% Tasa Soporte",
            "NPS_Numerico": "NPS Promedio"
        },
        title="Impacto del Descuido Operativo por Bodega"
    )

    fig.update_layout(
        margin=dict(l=60, r=40, t=60, b=40)
    )

    return fig


# =============================================================================
# FUNCIÓN DE UI: Métricas y gráficos en Streamlit
# =============================================================================
def mostrar_riesgo_operativo(df_filtrado: pd.DataFrame, renderizar: bool = True):

    df = df_filtrado.copy()
    df["Ultima_Revision"] = pd.to_datetime(df["Ultima_Revision"], errors="coerce")

    fecha_referencia = pd.to_datetime(datetime.now().date())
    df["dias_sin_revision"] = (fecha_referencia - df["Ultima_Revision"]).dt.days

    fig_riesgo = construir_fig_riesgo_operativo(df)

    if renderizar:
        st.header("⚠️ Riesgo Operativo: Bodegas 'A Ciegas'")

        col1, col2, col3 = st.columns(3)

        with col1:
            promedio_dias = df["dias_sin_revision"].mean()
            st.metric(
                "📅 Promedio Días Sin Revisión",
                f"{promedio_dias:.0f} días" if not np.isnan(promedio_dias) else "N/A"
            )

        with col2:
            tasa_soporte = df["Ticket_Soporte"].mean() * 100
            st.metric(
                "🎫 Tasa de Tickets de Soporte",
                f"{tasa_soporte:.1f}%" if not np.isnan(tasa_soporte) else "N/A"
            )

        with col3:
            df_corr = df.dropna(subset=["dias_sin_revision", "NPS_Numerico"])
            correlacion = (
                df_corr["dias_sin_revision"].corr(df_corr["NPS_Numerico"])
                if not df_corr.empty else np.nan
            )
            st.metric(
                "📈 Correlación Riesgo/NPS",
                f"{correlacion:.2f}" if not np.isnan(correlacion) else "N/A",
                help="Mide si el aumento en días sin revisión impacta el NPS."
            )

        st.markdown("---")

        st.subheader("🕵️ Relación: Antigüedad de Revisión vs. Incidencias")
        st.plotly_chart(fig_riesgo, use_container_width=True)

        st.markdown("---")
        st.subheader("🏭 Top Bodegas en Riesgo Crítico")

        df_bodegas = (
            df
            .dropna(subset=["dias_sin_revision"])
            .groupby("Bodega_Origen")
            .agg(
                Dias_Sin_Revision=("dias_sin_revision", "mean"),
                Tasa_Tickets_Soporte=("Ticket_Soporte", lambda x: x.mean() * 100),
                Ingresos_Expuestos=("ingreso_total", "sum")
            )
            .reset_index()
        )

        if df_bodegas.empty:
            st.info("No hay información suficiente para identificar bodegas en riesgo.")
        else:
            df_bodegas = df_bodegas.sort_values(
                by=["Dias_Sin_Revision", "Tasa_Tickets_Soporte"],
                ascending=[False, False]
            ).head(5)

            st.dataframe(
                df_bodegas.style.format({
                    "Dias_Sin_Revision": "{:.0f}",
                    "Tasa_Tickets_Soporte": "{:.1f}%",
                    "Ingresos_Expuestos": "${:,.0f}"
                }),
                use_container_width=True,
                hide_index=True
            )

            st.caption(
                "🔎 **Criterio:** bodegas con mayor antigüedad de revisión y alta incidencia "
                "de tickets de soporte representan un riesgo operativo crítico."
            )
    return fig_riesgo

