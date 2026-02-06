# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px


def _metric_value(metricas: dict, *keys, default=0):
    for key in keys:
        if key in metricas:
            return metricas.get(key, default)
    return default

def mostrar_salud_datos(df, metricas_calidad):
    st.header("🔍 Salud del Dato - Auditoría de Calidad")
    st.markdown("---")
    
    # 1. Preparación de datos para los gráficos
    hs_data = []
    for modulo, met in metricas_calidad.items():
        hs_data.append({
            "Módulo": modulo.capitalize(),
            "Antes": met.get("health_score_antes", 0),
            "Despues": met.get("health_score_despues", 0)
        })
    df_hs = pd.DataFrame(hs_data)

    # 2. Resumen Ejecutivo
    avg_antes = df_hs["Antes"].mean()
    avg_despues = df_hs["Despues"].mean()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("⭐ Health Score Inicial", f"{avg_antes:.1f}%")
    with col2:
        st.metric("✅ Health Score Final", f"{avg_despues:.1f}%")
        st.markdown(
            f"<div class='kpi-percentage'>+{avg_despues - avg_antes:.1f}% mejora</div>",
            unsafe_allow_html=True,
        )
    with col3:
        nulos = df.isna().sum().sum()
        st.metric("🕳️ Celdas Vacías", f"{nulos:,}")

    # 3. Gráfico Comparativo
    fig = px.bar(df_hs, x="Módulo", y=["Antes", "Despues"], barmode="group",
                 title="Mejora de Calidad por Módulo",
                 color_discrete_map={"Antes": "#93bedf", "Despues": "#1f4e78"})
    st.plotly_chart(fig, use_container_width=True)

    # 4. Detalle por Módulo (Tabs)
    t1, t2, t3 = st.tabs(["Feedback", "Inventario", "Transacciones"])
    
    with t1:
        m = metricas_calidad.get("feedback", {})
        c1, c2 = st.columns(2)
        c1.metric("👤 Edades Corregidas", _metric_value(m, "edades_corregidas"))
        c2.metric("⭐ Ratings Ajustados", _metric_value(m, "ratings_corregidos"))
        st.info("Estrategia: Normalización de NPS a base 10 e imputación de edades por mediana.")

    with t2:
        m = metricas_calidad.get("inventario", {})
        c1, c2 = st.columns(2)
        c1.metric("💰 Costos Atípicos", _metric_value(m, "costos_outliers", "costos_outliers_detectados"))
        c2.metric("📦 Stocks Negativos", _metric_value(m, "stock_negativos", "stock_negativos_corregidos"))
        st.info("Estrategia: Limpieza de costos mediante mediana por categoría.")

    with t3:
        m = metricas_calidad.get("transacciones", {})
        c1, c2 = st.columns(2)
        c1.metric("🚚 Tiempos 'Outliers'", _metric_value(m, "tiempos_outliers"))
        c2.metric("❌ SKUs No Catalogados", _metric_value(m, "skus_sin_inventario"))
        st.info("Estrategia: Corrección de tiempos de entrega de 999 días.")