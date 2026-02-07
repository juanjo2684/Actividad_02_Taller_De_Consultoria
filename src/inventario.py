# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

# -----------------------------
# Constantes y configuraciones
# -----------------------------

CATEGORIAS_NORMALIZADAS = {
    "laptops": "laptop",
    "smart-phone": "smartphone",
    "smartphones": "smartphone",
    "???": np.nan,
    "unknown": np.nan,
    "sin categoria": np.nan
}

# -----------------------------
# Utilidades
# -----------------------------

def iqr_bounds(series: pd.Series):
    """Calcula límites para detección de outliers usando IQR"""
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return lower_bound, upper_bound


def select_max_lead_time(value):
    """
    Extrae el valor numérico máximo de lead time desde strings variados.
    Ejemplos: "25-30" → 30, "inmediato" → 1, "nan" → pd.NA
    """
    if pd.isna(value):
        return pd.NA
    
    s = str(value).strip().lower()
    
    if s in ("", "nan", "none", "null"):
        return pd.NA
    
    if "inmediato" in s:
        return 1
    
    numbers = re.findall(r"\d+", s)
    if not numbers:
        return pd.NA
    
    try:
        return max(int(num) for num in numbers)
    except (ValueError, TypeError):
        return pd.NA


def calcular_health_score(df):
    """
    Calcula Health Score según fórmula: 100 × (1 - (0.7 × % Nulos + 0.3 × % Duplicados))
    """
    if df.empty:
        return 0, 0, 0
        
    total_celdas = df.shape[0] * df.shape[1]
    total_nulos = df.isna().sum().sum()
    porcentaje_nulos = total_nulos / total_celdas if total_celdas > 0 else 0
    
    duplicados = df.duplicated().sum()
    porcentaje_duplicados = duplicados / len(df) if len(df) > 0 else 0
    
    health_score = 100 * (1 - (0.7 * porcentaje_nulos + 0.3 * porcentaje_duplicados))
    
    return round(health_score, 2), round(porcentaje_nulos * 100, 2), round(porcentaje_duplicados * 100, 2)


# -----------------------------
# Función principal
# -----------------------------

def procesar_inventario(inventario_path: str) -> tuple:
    
    # 1. Carga y auditoría inicial
    try:
        inventario_raw = pd.read_csv(inventario_path)
    except Exception as e:
        return pd.DataFrame(), {"error": str(e)}

    # Limpieza de nombres de columnas
    inventario_raw.columns = [c.strip() for c in inventario_raw.columns]
    
    health_antes, pct_nulos_antes, pct_dups_antes = calcular_health_score(inventario_raw)
    
    # Normalización de columnas críticas
    if "Bodega_Origen" not in inventario_raw.columns:
        bodega_cols = [col for col in inventario_raw.columns if 'bodega' in col.lower()]
        if bodega_cols:
            inventario_raw = inventario_raw.rename(columns={bodega_cols[0]: "Bodega_Origen"})
    
    # 2. Limpieza de Strings, Fechas y NORMALIZACIÓN
    df_inventario = inventario_raw.copy()
    
    if "Bodega_Origen" in df_inventario.columns:
        df_inventario["Bodega_Origen"] = (
            df_inventario["Bodega_Origen"]
            .astype(str)
            .str.strip()
            .str.upper()
        )
        
        mapeo_bodegas = {"NORTE": "Norte", "SUR": "Sur", "CENTRO": "Centro"}
        df_inventario["Bodega_Origen"] = df_inventario["Bodega_Origen"].replace(mapeo_bodegas)
    
    # Normalización de Categoría
    df_inventario["Categoria"] = df_inventario["Categoria"].astype(str).str.lower().str.strip()
    df_inventario["Categoria"] = df_inventario["Categoria"].replace(CATEGORIAS_NORMALIZADAS)
    
    # Procesamiento de Lead Time
    df_inventario["Lead_Time_Dias"] = df_inventario["Lead_Time_Dias"].map(select_max_lead_time)
    
    # Conversión de fecha robusta
    df_inventario["Ultima_Revision"] = pd.to_datetime(df_inventario["Ultima_Revision"], errors="coerce")
    
    # 3. Corrección de Stock Negativo
    stock_negativos = (df_inventario["Stock_Actual"] < 0).sum()
    df_inventario["Stock_Actual"] = pd.to_numeric(df_inventario["Stock_Actual"], errors="coerce").fillna(0).abs()
    
    # 4. Auditoría y Corrección de Costos (IQR por Categoría)
    df_inventario["Costo_Unitario_USD"] = pd.to_numeric(df_inventario["Costo_Unitario_USD"], errors="coerce")
    
    # Lógica para evitar errores si no hay suficientes datos para IQR
    try:
        lower_b, upper_b = iqr_bounds(df_inventario["Costo_Unitario_USD"].dropna())
        mascara_outliers = (df_inventario["Costo_Unitario_USD"] < lower_b) | (df_inventario["Costo_Unitario_USD"] > upper_b)
    except:
        mascara_outliers = pd.Series([False] * len(df_inventario))
        
    costos_outliers_total = mascara_outliers.sum()
    
    # Imputación estratégica: Mediana por categoría
    costo_mediana_cat = df_inventario.groupby("Categoria")["Costo_Unitario_USD"].transform("median")
    df_inventario.loc[mascara_outliers, "Costo_Unitario_USD"] = costo_mediana_cat[mascara_outliers]
    # Si aún hay nulos (categorías sin mediana), usar la mediana global
    df_inventario["Costo_Unitario_USD"] = df_inventario["Costo_Unitario_USD"].fillna(df_inventario["Costo_Unitario_USD"].median())
    
    # 5. Imputación de Lead Time (Mediana por categoría)
    df_inventario["Lead_Time_Dias"] = df_inventario.groupby("Categoria")["Lead_Time_Dias"].transform(lambda x: x.fillna(x.median()))
    df_inventario["Lead_Time_Dias"] = df_inventario["Lead_Time_Dias"].fillna(df_inventario["Lead_Time_Dias"].median())
    
    # 6. Métricas de Calidad y Negocio Finales
    health_despues, pct_nulos_despues, pct_dups_despues = calcular_health_score(df_inventario)
    
    metricas = {
        "dataset": "Inventario",
        "health_score_antes": health_antes,
        "health_score_despues": health_despues,
        "mejora_health_score": round(health_despues - health_antes, 2),
        "costos_outliers_detectados": int(costos_outliers_total),
        "stock_negativos_corregidos": int(stock_negativos),
        "costos_outliers": int(costos_outliers_total),
        "stock_negativos": int(stock_negativos),
        "duplicados_sku_id": int(df_inventario.duplicated(subset=["SKU_ID"]).sum()),
        "valor_inventario_total": f"${(df_inventario['Stock_Actual'] * df_inventario['Costo_Unitario_USD']).sum():,.2f}",
        "rango_costos_final": f"${df_inventario['Costo_Unitario_USD'].min():.2f} - ${df_inventario['Costo_Unitario_USD'].max():.2f}"
    }

      

    return df_inventario, metricas


