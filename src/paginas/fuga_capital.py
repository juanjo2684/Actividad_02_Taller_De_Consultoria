# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def mostrar_fuga_capital(df_filtrado):

    st.header("💰 Fuga de Capital y Rentabilidad")
    
    # 1. Identificación de Pérdidas (Solo registros con margen < 0)
    df_perdida = df_filtrado[df_filtrado["margen_real"] < 0].copy()
    total_fuga = df_perdida["margen_real"].sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("💸 Fuga Total (USD)", f"${abs(total_fuga):,.2f}")
    with col2:
        st.metric("📦 SKUs en Pérdida", f"{df_perdida['SKU_ID'].nunique()}")
    with col3:
        ingresos_totales = df_filtrado["ingreso_total"].sum()
        impacto_ingresos = (abs(total_fuga) / ingresos_totales * 100) if ingresos_totales > 0 else 0
        st.metric("% Impacto sobre Ingresos", f"{impacto_ingresos:.2f}%")

    st.markdown("---")

    # 2. Matriz de Riesgo (Dispersión)
    st.subheader("🔍 Análisis de Riesgo: ¿Volumen o Falla de Precio?")
    df_sku_risk = df_filtrado.groupby(["SKU_ID", "Categoria"]).agg({
        "margen_real": "sum",
        "ingreso_total": "sum",
        "Cantidad_Vendida": "sum"
    }).reset_index()
    df_sku_risk["size_burbuja"] = df_sku_risk["Cantidad_Vendida"].fillna(0).abs() + 0.1

    fig_risk = px.scatter(
        df_sku_risk, x="ingreso_total", y="margen_real",
        size="size_burbuja", color="margen_real",
        color_continuous_scale="BuPu", color_continuous_midpoint=0,
        hover_name="SKU_ID", title="Matriz de Dispersión: Margen vs. Ingresos por SKU"
    )
    fig_risk.add_hline(y=0, line_dash="dash", line_color="black")
    st.plotly_chart(fig_risk, use_container_width=True)

    # 3. Rendimiento Porcentual (Promedios)
    st.subheader("🌐 Eficiencia Relativa por Canal")
    canal_col = "Canal_Venta" if "Canal_Venta" in df_filtrado.columns else "Bodega_Origen"
    
    df_canal = df_filtrado.groupby(canal_col).agg({
        "margen_real": "sum",
        "ingreso_total": "sum"
    }).reset_index()
    df_canal["%_Margen"] = df_canal.apply(lambda x: (x["margen_real"] / x["ingreso_total"] * 100) if x["ingreso_total"] > 0 else 0, axis=1)

    fig_canal = px.bar(
        df_canal, x=canal_col, y="%_Margen", color="%_Margen",
        color_continuous_scale="BuPu", color_continuous_midpoint=0,
        title="Rendimiento de Margen Promedio (%)", text_auto=".2f"
    )
    st.plotly_chart(fig_canal, use_container_width=True)

    # 3.1. CONSOLIDADO DE FUGA POR CANAL
    st.subheader("📉 Magnitud de la Falla: Fuga de Capital por Canal")
    if not df_perdida.empty:
        # Sumamos solo las pérdidas económicas por canal
        fuga_por_canal = df_perdida.groupby(canal_col)["margen_real"].sum().reset_index()
        fuga_por_canal["margen_real"] = fuga_por_canal["margen_real"].abs()
        fuga_por_canal = fuga_por_canal.sort_values("margen_real", ascending=False)

        fig_fuga_cons = px.bar(
            fuga_por_canal,
            x=canal_col,
            y="margen_real",
            color="margen_real",
            color_continuous_scale="Blues",
            title="Consolidado de Dinero Perdido (USD) por Canal",
            labels={"margen_real": "Fuga Total (USD)", canal_col: "Canal de Venta"},
            text_auto=":,.0f"
        )
        st.plotly_chart(fig_fuga_cons, use_container_width=True)
    else:
        st.success("No se detecta fuga de capital acumulada.")

    # 4. Top 10 SKUs Críticos
    st.subheader("🚨 Top 10 SKUs con Mayor Pérdida (Global)")
    if not df_perdida.empty:
        top_fugas = df_perdida.groupby("SKU_ID").agg({
            "Categoria": "first",
            "margen_real": "sum",
            "Cantidad_Vendida": "sum",
            "Precio_Venta_Final": "mean"
        }).sort_values("margen_real").head(10)
        
        st.table(top_fugas.style.format({
            "margen_real": "${:,.2f}", 
            "Precio_Venta_Final": "${:,.2f}",
            "Cantidad_Vendida": "{:,.0f}"
        }))

    # 5. Recomendación de Consultoría
    with st.expander("💡 Diagnóstico del Consultor"):
        impacto = (abs(total_fuga) / ingresos_totales) if ingresos_totales > 0 else 0
        if impacto > 0.05:
            peor_canal = df_canal.loc[df_canal["margen_real"].idxmin(), canal_col]
            st.error(f"⚠️ **Falla Crítica:** El impacto del {impacto*100:.2f}% de ingresos concentrado en el canal **{peor_canal}** requiere revisión de la política de fletes.")
        else:
            st.success("✅ Operación bajo control estadístico tras curaduría de datos.")