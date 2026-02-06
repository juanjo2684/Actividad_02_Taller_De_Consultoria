# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px

def mostrar_resumen_ejecutivo(df_filtrado, health_scores, metricas_calidad):

    st.header("📈 Resumen Ejecutivo")
    st.markdown("---")
    
    # -----------------------------
    # 1. KPIs principales
    # -----------------------------
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        ingresos_totales = df_filtrado["ingreso_total"].sum()
        st.metric("💰 Ingresos Totales", f"${ingresos_totales:,.0f}")
    
    with col2:
        margen_total = df_filtrado["margen_real"].sum()
        margen_pct = (margen_total / ingresos_totales * 100) if ingresos_totales != 0 else 0
        st.metric("📊 Margen Neto", f"${margen_total:,.0f}")
        st.markdown(
            f"<div class='kpi-percentage'>{margen_pct:.1f}%</div>",
            unsafe_allow_html=True
        )
    
    with col3:
        ventas_sin_inventario = df_filtrado["venta_sin_inventario"].sum()
        pct_riesgo = (ventas_sin_inventario / len(df_filtrado) * 100) if len(df_filtrado) > 0 else 0
        st.metric("👻 Ventas Sin Inventario", f"{ventas_sin_inventario:,}")
        st.markdown(
            f"<div class='kpi-percentage'>{pct_riesgo:.1f}% Riesgo</div>",
            unsafe_allow_html=True
        )
    
    with col4:
        margen_negativo = (df_filtrado["margen_real"] < 0).sum()
        st.metric("🔴 Transacciones con Pérdida", f"{margen_negativo:,}")
    
    st.markdown("---")
    
    # -----------------------------
    # 2. Health Scores
    # -----------------------------
    st.subheader("📊 Health Score de Datos")
    
    hs_data = []
    for dataset, scores in health_scores.items():
        hs_data.append({
            "Dataset": dataset,
            "Antes": scores["Antes"],
            "Despues": scores["Despues"],
            "Mejora": scores["Despues"] - scores["Antes"]
        })
    
    df_hs = pd.DataFrame(hs_data)
    
    col_a, col_b = st.columns([1, 2])
    
    with col_a:
        st.dataframe(df_hs.style.format({
            "Antes": "{:.1f}",
            "Despues": "{:.1f}",
            "Mejora": "{:+.1f}"
        }), hide_index=True)
    
    with col_b:
        df_hs_melted = df_hs.melt(id_vars=["Dataset"], value_vars=["Antes", "Despues"], var_name="Estado", value_name="Score")
        fig_hs = px.bar(
            df_hs_melted, x="Dataset", y="Score", color="Estado",
            barmode="group", height=300,
            color_discrete_map={"Antes": "#93bedf", "Despues": "#1f4e78"}
        )
        st.plotly_chart(fig_hs, use_container_width=True)

    st.markdown("---")
    
    # -----------------------------
    # 3. Top categorías y Alerta de Auditoría
    # -----------------------------
    st.subheader("🏆 Top Categorías por Ingresos")

    top_categorias = df_filtrado.groupby("Categoria").agg({
        "ingreso_total": "sum",
        "margen_real": "sum",
        "Transaccion_ID": "count"
    }).rename(columns={
        "ingreso_total": "Ingresos",
        "margen_real": "Margen",
        "Transaccion_ID": "Transacciones"
    })
    
    top_categorias["Margen %"] = (top_categorias["Margen"] / top_categorias["Ingresos"] * 100).round(1)
    top_df = top_categorias.nlargest(5, "Ingresos").reset_index()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig_cat = px.bar(
            top_df, x="Categoria", y="Ingresos", color="Margen %",
            color_continuous_scale="BuPu",
            title="Distribución de Ingresos y Rentabilidad",
            hover_data=["Transacciones", "Margen"]
        )
        st.plotly_chart(fig_cat, use_container_width=True)
        
        if "No Catalogado" in top_df["Categoria"].values:
            st.warning("⚠️ **Nota de Auditoría:** La categoría 'No Catalogado' muestra un margen inflado (~99%) debido a la ausencia de costos unitarios en el maestro de inventario.")
    
    with col2:
        st.write("**Detalle de Rendimiento**")
        st.dataframe(
            top_df[["Categoria", "Ingresos", "Margen %"]].set_index("Categoria").style.format({
                "Ingresos": "${:,.0f}",
                "Margen %": "{:.1f}%"
            }), height=300
        )
    
    st.markdown("---")
    
    # -----------------------------
    # 4. Exportación de Reportes
    # -----------------------------
    st.subheader("📤 Exportar Datos Filtrado y Metricas de Calidad")
    
    col_c1, col_c2, col_c3 = st.columns(3)
    
    with col_c1:
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Datos Filtrados (CSV)", data=csv, file_name="datos_filtrados.csv", mime="text/csv")
    
    with col_c2:
        metricas_df = pd.DataFrame(metricas_calidad).T
        csv_metricas = metricas_df.to_csv().encode('utf-8')
        st.download_button("📊 Métricas Calidad (CSV)", data=csv_metricas, file_name="metricas_calidad.csv", mime="text/csv")
    
 