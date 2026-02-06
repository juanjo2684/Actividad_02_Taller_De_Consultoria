# -*- coding: utf-8 -*-
import os
import tempfile
import pandas as pd
import streamlit as st
from src.inventario import procesar_inventario
from src.transacciones import procesar_transacciones
from src.feedback import procesar_feedback

pd.set_option('future.no_silent_downcasting', True)

# Rutas por defecto dentro del repositorio
_DEFAULT_INVENTARIO = "data/inventario_central_v2.csv"
_DEFAULT_FEEDBACK = "data/feedback_clientes_v2.csv"
_DEFAULT_TRANSACCIONES = "data/transacciones_logistica_v2.csv"


def render_file_upload_section() -> tuple:
    """Muestra uploaders en el sidebar y devuelve las rutas a utilizar."""
    st.sidebar.markdown("---")
    st.sidebar.subheader("📂 Archivos de Datos")
    st.sidebar.caption(
        "Por defecto se usan los archivos incluidos en el repositorio. "
        "Puedes cargar tus propios CSV para reemplazarlos."
    )

    up_inv = st.sidebar.file_uploader(
        "Inventario CSV", type=["csv"], key="upload_inventario",
        help="inventario_central_v2.csv",
    )
    up_feed = st.sidebar.file_uploader(
        "Feedback CSV", type=["csv"], key="upload_feedback",
        help="feedback_clientes_v2.csv",
    )
    up_trans = st.sidebar.file_uploader(
        "Transacciones CSV", type=["csv"], key="upload_transacciones",
        help="transacciones_logistica_v2.csv",
    )

    def _resolve(uploaded, default_path: str) -> str:
        if uploaded is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            tmp.write(uploaded.getvalue())
            tmp.close()
            return tmp.name
        return default_path

    ruta_inv = _resolve(up_inv, _DEFAULT_INVENTARIO)
    ruta_feed = _resolve(up_feed, _DEFAULT_FEEDBACK)
    ruta_trans = _resolve(up_trans, _DEFAULT_TRANSACCIONES)

    return ruta_inv, ruta_feed, ruta_trans


@st.cache_data
def cargar_datos(ruta_inventario: str, ruta_feedback: str, ruta_transacciones: str):
    # 1. Carga de archivos individuales con sus respectivas métricas de salud
    df_inv, met_inv = procesar_inventario(ruta_inventario)
    df_feed, met_feed = procesar_feedback(ruta_feedback)
    df_trans, met_trans = procesar_transacciones(ruta_transacciones, df_inv, df_feed)
    
    # 2. Consolidación en un único Dataset Maestro para el DSS
    df_dss = crear_dataset_consolidado(df_trans, df_inv, df_feed)
    
    # 3. Diccionarios de salud para las pestañas de Resumen y Salud del Dato
    health_scores = {
        "Inventario": {"Antes": met_inv.get("health_score_antes", 0), "Despues": met_inv.get("health_score_despues", 0)},
        "Transacciones": {"Antes": met_trans.get("health_score_antes", 0), "Despues": met_trans.get("health_score_despues", 0)},
        "Feedback": {"Antes": met_feed.get("health_score_antes", 0), "Despues": met_feed.get("health_score_despues", 0)}
    }
    
    metricas_calidad = {
        "inventario": met_inv, 
        "transacciones": met_trans, 
        "feedback": met_feed
    }
    
    return df_dss, health_scores, metricas_calidad

def crear_dataset_consolidado(df_trans, df_inv, df_feed):
    df_trabajo = df_trans.copy()

    # --- 1. Rescate de Tiempo_Entrega ---
    if 'Tiempo_Entrega' not in df_trabajo.columns:
        posibles = [c for c in df_trabajo.columns if 'tiempo' in c.lower() or 'entrega' in c.lower()]
        if posibles:
            df_trabajo = df_trabajo.rename(columns={posibles[0]: 'Tiempo_Entrega'})
        else:
            df_trabajo['Tiempo_Entrega'] = 0

    # --- 2. Cruce con Inventario ---
    df_merged = df_trabajo.merge(
        df_inv[['SKU_ID', 'Categoria', 'Costo_Unitario_USD', 'Punto_Reorden', 'Stock_Actual', 'Bodega_Origen', 'Lead_Time_Dias', 'Ultima_Revision']],
        on="SKU_ID", how="left"
    )
    
    # --- 3. Cruce con Feedback ---
    columnas_deseadas = [
        'Transaccion_ID', 'NPS_Numerico', 'NPS_Categoria', 
        'Rating_Producto', 'Edad_Cliente', 'Ticket_Soporte'
    ]
    
    cols_presentes = [c for c in columnas_deseadas if c in df_feed.columns]
    df_feed_clean = df_feed[cols_presentes].drop_duplicates(subset=['Transaccion_ID'])
    
    df_final = df_merged.merge(df_feed_clean, on="Transaccion_ID", how="left")
    
    # --- 4. Rellenos de seguridad ---
    df_final["Categoria"] = df_final["Categoria"].fillna("no catalogado")
    df_final["venta_sin_inventario"] = df_final["Categoria"] == "no catalogado"
    df_final["NPS_Numerico"] = pd.to_numeric(df_final["NPS_Numerico"], errors='coerce').fillna(5.0)
    df_final["Stock_Actual"] = df_final["Stock_Actual"].fillna(0)
    df_final["Tiempo_Entrega"] = df_final["Tiempo_Entrega"].fillna(0)
    
    # Asegurar que Ticket_Soporte sea numérico
    df_final["Ticket_Soporte"] = pd.to_numeric(df_final["Ticket_Soporte"], errors='coerce').fillna(0).astype(int)
    
    # --- 5. Cálculos Financieros Operativos ---
    df_final["ingreso_total"] = df_final["Precio_Venta_Final"] * df_final["Cantidad_Vendida"]
    costo_u = df_final["Costo_Unitario_USD"].fillna(0)
    df_final["costo_total"] = (costo_u * df_final["Cantidad_Vendida"]) + df_final["Costo_Envio"]
    df_final["margen_real"] = df_final["ingreso_total"] - df_final["costo_total"]
    
    # --- 6. Análisis de Brecha Logística ---
    if "Lead_Time_Dias" in df_final.columns:
        df_final["brecha_entrega"] = df_final["Tiempo_Entrega"] - df_final["Lead_Time_Dias"].fillna(0)
    else:
        df_final["brecha_entrega"] = 0

    # --- 7. Lógica de la Paradoja de Fidelidad ---
    stock_q3 = df_final["Stock_Actual"].quantile(0.75) if len(df_final) > 0 else 0
    df_final["paradoja_fidelidad"] = (df_final["Stock_Actual"] > stock_q3) & (df_final["NPS_Numerico"] < 7)

    return df_final