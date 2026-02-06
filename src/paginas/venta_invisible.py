# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px


# =============================================================================
# FUNCIÓN PURA: Construcción de la figura
# =============================================================================
def construir_fig_venta_invisible(df_filtrado: pd.DataFrame):

    df = df_filtrado.copy()

    columnas_requeridas = {"venta_sin_inventario", "Ciudad_Destino", "ingreso_total"}
    if not columnas_requeridas.issubset(df.columns):
        fig = px.bar(title="Top 10 Ciudades con Ventas Invisibles")
        fig.add_annotation(
            text="Columnas requeridas no disponibles para generar la gráfica",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False
        )
        return fig

    df_sin_inv = df[df["venta_sin_inventario"]].copy()

    # Caso sin datos
    if df_sin_inv.empty:
        fig = px.bar(
            title="Top 10 Ciudades con Ventas Invisibles",
            labels={"value": "Ingresos (USD)", "Ciudad_Destino": "Ciudad"}
        )
        fig.add_annotation(
            text="No hay ventas invisibles para los filtros actuales",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False
        )
        return fig

    # Agregación
    fuga_ciudad = (
        df_sin_inv
        .groupby("Ciudad_Destino", dropna=False)["ingreso_total"]
        .sum()
        .sort_values(ascending=True)
        .tail(10)
    )

    fig = px.bar(
        fuga_ciudad,
        orientation="h",
        title="Top 10 Ciudades con Mayor Fuga por Venta Invisible",
        labels={
            "value": "Ingreso en Riesgo (USD)",
            "Ciudad_Destino": "Ciudad Destino"
        }
    )

    fig.update_traces(
        marker_color="#1f4e78",
        texttemplate="$%{x:,.0f}",
        textposition="outside"
    )

    fig.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(
            family="Helvetica",
            size=11,
            color="black"
        ),
        yaxis=dict(
            automargin=True,
            tickfont=dict(size=10),
            title_standoff=25
        ),
        xaxis=dict(
            tickfont=dict(size=10),
            title_font=dict(size=12)
        ),
        margin=dict(
            l=180, 
            r=40,
            t=60,
            b=40
        )
    )

    return fig

# =============================================================================
# FUNCIÓN DE UI: renderiza métricas y gráficos en Streamlit
# =============================================================================
def mostrar_venta_invisible(df_filtrado: pd.DataFrame, renderizar: bool = True):

    df = df_filtrado.copy()
    df_sin_inv = df[df["venta_sin_inventario"]].copy()

    ingreso_riesgo = df_sin_inv["ingreso_total"].sum() if not df_sin_inv.empty else 0
    total_general = df["ingreso_total"].sum()
    pct_ingreso_riesgo = (
        ingreso_riesgo / total_general * 100
        if total_general != 0 else 0
    )
    skus_huerfanos = df_sin_inv["SKU_ID"].nunique() if not df_sin_inv.empty else 0

    fig_city = construir_fig_venta_invisible(df)

    if renderizar:
        st.header("👻 Análisis de la Venta Invisible")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "💰 Ingreso en Riesgo (USD)",
                f"${ingreso_riesgo:,.2f}",
            )
            st.markdown(
                f"<div class='kpi-percentage'>{pct_ingreso_riesgo:.1f}% del Total</div>",
                unsafe_allow_html=True,
            )
        with col2:
            st.metric("🆔 SKUs No Catalogados", f"{skus_huerfanos}")
        with col3:
            st.metric("📝 Transacciones Afectadas", f"{len(df_sin_inv):,}")

        st.markdown("---")

        st.subheader("📅 Evolución del Riesgo de Inventario")
        if not df_sin_inv.empty and "Fecha_Venta" in df_sin_inv.columns:
            df_tiempo = (
                df_sin_inv
                .groupby(df_sin_inv["Fecha_Venta"].dt.to_period("M"))
                .agg(
                    ingreso_total=("ingreso_total", "sum"),
                    transacciones=("Transaccion_ID", "count")
                )
                .reset_index()
            )
            df_tiempo["Fecha_Venta"] = df_tiempo["Fecha_Venta"].astype(str)

            fig_line = px.line(
                df_tiempo,
                x="Fecha_Venta",
                y="ingreso_total",
                title="Ingresos por Ventas Invisibles por Mes",
                labels={"ingreso_total": "Ingresos (USD)", "Fecha_Venta": "Mes"},
                markers=True
            )
            fig_line.update_traces(line=dict(color="#2e75b6", width=2))
            fig_line.update_layout(margin=dict(l=40, r=20, t=50, b=40))

            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info("No hay datos suficientes para mostrar la evolución temporal.")

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("📍 Fuga por Ciudad")
            st.plotly_chart(fig_city, use_container_width=True)

        with col_b:
            st.subheader("🏭 Impacto por Canal/Bodega")
            if not df_sin_inv.empty:
                col_ref = (
                    "Canal_Venta"
                    if "Canal_Venta" in df_sin_inv.columns
                    else "Bodega_Origen"
                )
                fuga_canal = (
                    df_sin_inv
                    .groupby(col_ref)["ingreso_total"]
                    .sum()
                    .sort_values(ascending=False)
                )
                fig_pie = px.pie(
                    values=fuga_canal.values,
                    names=fuga_canal.index,
                    title=f"Distribución por {col_ref}",
                    color_discrete_sequence=["#1f4e78", "#2e75b6", "#00a1d6", "#7f3fbf", "#f39c12", "#c0392b"]
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No hay datos suficientes para análisis por canal.")

        with st.expander("💡 Conclusión del Consultor"):
            if pct_ingreso_riesgo > 10:
                st.error(
                    f"⚠️ ALERTA CRÍTICA: "
                    f"{pct_ingreso_riesgo:.1f}% de los ingresos sin trazabilidad."
                )
            elif pct_ingreso_riesgo > 5:
                st.warning("🟡 Riesgo moderado de catalogación.")
            else:
                st.success("✅ Riesgo bajo dentro de límites operativos.")

    return fig_city
