# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

def mostrar_crisis_logistica(df_filtrado):

    st.header("🚚 Crisis Logística y Cuellos de Botella")
    
    # ---------------------------------------------------------
    # 1. Preparación de Datos
    # ---------------------------------------------------------
    df_log = df_filtrado.dropna(subset=["Tiempo_Entrega", "NPS_Numerico"]).copy()
    
    df_log["Ciudad_Destino"] = df_log["Ciudad_Destino"].astype(str).str.strip().str.upper()

    df_analisis = df_log[
        (df_log["Tiempo_Entrega"] < 100) & 
        (df_log["Tiempo_Entrega"] > 0)
    ].copy()

    if df_analisis.empty:
        df_analisis = df_log[df_log["Tiempo_Entrega"] < 100].copy()

    filtro_canal = df_analisis["Ciudad_Destino"].str.contains("CANAL DIGITAL|DIGITAL", na=False)
    registros_canal_digital = df_analisis[filtro_canal].shape[0]

    df_geo = df_analisis[~filtro_canal].copy()

    # ---------------------------------------------------------
    # 2. KPIs de Desempeño Logístico
    # ---------------------------------------------------------
    col1, col2, col3 = st.columns(3)
    with col1:
        tiempo_avg = df_analisis["Tiempo_Entrega"].mean()
        st.metric("⏳ Tiempo Entrega Prom.", f"{tiempo_avg:.1f} días")
    with col2:
        corr_global = df_analisis["Tiempo_Entrega"].corr(df_analisis["NPS_Numerico"])
        st.metric("🔗 Correlación NPS vs Tiempo", f"{corr_global:.2f}" if not np.isnan(corr_global) else "N/A")
    with col3:
        brecha_max = df_analisis["brecha_entrega"].max() if "brecha_entrega" in df_analisis.columns else 0
        st.metric("🚩 Brecha Máxima", f"{brecha_max:.0f} días")

    if registros_canal_digital > 0:
        st.info(f"🔎 **Hallazgo de Auditoría:** Se detectaron {registros_canal_digital} registros etiquetados como 'CANAL DIGITAL'. "
                "Estos han sido removidos de las gráficas de rutas físicas para normalizar el análisis geográfico.")

    st.markdown("---")

    # ---------------------------------------------------------
    # 3. Identificación de la Zona Crítica
    # ---------------------------------------------------------
    st.subheader("📍 Mapa de Calor: ¿En qué ruta física fallamos?")
    
    df_rutas = df_geo.groupby(["Bodega_Origen", "Ciudad_Destino"]).agg({
        "NPS_Numerico": "mean",
        "Tiempo_Entrega": "mean",
        "Transaccion_ID": "count"
    }).reset_index()

    if not df_rutas.empty:
        df_rutas["score_crisis"] = df_rutas["Tiempo_Entrega"] / (df_rutas["NPS_Numerico"] + 0.1)

        df_rutas = df_rutas.sort_values("Ciudad_Destino")

        fig_heat = px.density_heatmap(
            df_rutas, 
            x="Ciudad_Destino", 
            y="Bodega_Origen", 
            z="score_crisis",
            color_continuous_scale="Blues",
            title="Intensidad de Crisis por Ruta Geográfica (Incluye NPS 5.0)",
            labels={"score_crisis": "Índice de Crisis"}
        )
        
        fig_heat.update_xaxes(type='category')
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.warning("No hay suficientes datos geográficos limpios para generar el mapa.")

    # ---------------------------------------------------------
    # 4. Análisis de Correlación por Ciudad
    # ---------------------------------------------------------
    st.subheader("📉 Correlación Específica por Ciudad")
    
    correlaciones_ciudad = []
    ciudades_validas = df_geo["Ciudad_Destino"].unique()
    
    for ciudad in ciudades_validas:
        df_c = df_geo[df_geo["Ciudad_Destino"] == ciudad]
        if len(df_c) >= 2: 
            corr = df_c["Tiempo_Entrega"].corr(df_c["NPS_Numerico"])
            if not np.isnan(corr):
                correlaciones_ciudad.append({"Ciudad": ciudad, "Correlacion": corr})
    
    if correlaciones_ciudad:
        df_corr_city = pd.DataFrame(correlaciones_ciudad).sort_values("Correlacion")
        fig_corr = px.bar(
            df_corr_city, 
            x="Correlacion", y="Ciudad", 
            orientation='h',
            color="Correlacion",
            color_continuous_scale="Blues",
            title="Impacto del Tiempo en el NPS por Ciudad"
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    # ---------------------------------------------------------
    # 5. Recomendación Ejecutiva
    # ---------------------------------------------------------
    if not df_rutas.empty:
        ruta_peor = df_rutas.sort_values("score_crisis", ascending=False).iloc[0]
        st.subheader("🚨 Recomendación de Intervención")
        with st.expander("📝 Dictamen del Consultor Logístico"):
            st.error(f"Priorizar auditoría en ruta: **{ruta_peor['Bodega_Origen']} ➔ {ruta_peor['Ciudad_Destino']}**.")
            st.write(f"- **Tiempo prom.:** {ruta_peor['Tiempo_Entrega']:.1f} días.")
            st.write(f"- **NPS Promedio:** {ruta_peor['NPS_Numerico']:.2f}")