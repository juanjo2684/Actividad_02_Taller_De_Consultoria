# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)

def normalizar_nps_dinamico(valor):
    """
    Transforma valores de NPS de cualquier escala (-100 a 100 o 1 a 10) 
    a una escala uniforme de 1 a 10.
    """
    try:
        n = float(valor)
        if pd.isna(n): return 5.0
        
        # Caso A: Escala -100 a 100
        if n > 10:
            return 5 + (n / 20)  # [10, 100] -> [5.5, 10]
        elif n < 0:
            return 5 + (n / 25)  # [-100, 0] -> [1, 5]
            
        # Caso B: Escala 1 a 10
        elif 0 <= n <= 10:
            return n
            
        return 5.0
    except:
        return 5.0

def procesar_feedback(ruta_csv):
    try:
        df_feedback = pd.read_csv(ruta_csv)
    except Exception as e:
        return pd.DataFrame(), {"error": str(e)}

    # 1. Limpieza de nombres de columnas
    df_feedback.columns = [c.strip() for c in df_feedback.columns]
    
    # 2. Cálculo de Calidad Inicial
    salud_antes = calcular_health_score(df_feedback)

    # 3. Transformación y Normalización de NPS
    df_feedback["NPS_Numerico"] = df_feedback["Satisfaccion_NPS"].apply(normalizar_nps_dinamico)

    # 4. Categorización NPS
    df_feedback["NPS_Categoria"] = df_feedback["NPS_Numerico"].apply(
        lambda x: "Promotor" if x >= 9 else ("Pasivo" if x >= 7 else "Detractor")
    )

    # 5. Limpieza de Rating_Producto
    rating_raw = pd.to_numeric(df_feedback["Rating_Producto"], errors='coerce')
    mask_rating_outlier = rating_raw > 5
    mask_rating_nulo = rating_raw.isna()
    ratings_corregidos = int((mask_rating_outlier | mask_rating_nulo).sum())

    df_feedback["Rating_Producto"] = rating_raw

    # Outlier handling (> 5)
    df_feedback.loc[mask_rating_outlier, "Rating_Producto"] = np.nan

    # Mediana robusta
    mediana_rating = df_feedback["Rating_Producto"].median()
    valor_relleno_rating = mediana_rating if not pd.isna(mediana_rating) else 3.0
    df_feedback["Rating_Producto"] = df_feedback["Rating_Producto"].fillna(valor_relleno_rating)

    # 6. Limpieza de Edad y Soporte
    edad_raw = pd.to_numeric(df_feedback["Edad_Cliente"], errors='coerce')
    edades_corregidas = int(edad_raw.isna().sum())
    df_feedback["Edad_Cliente"] = edad_raw.fillna(35)
    
    soporte_raw = df_feedback["Ticket_Soporte_Abierto"].astype(str).str.strip().str.upper()
    
    mapeo_soporte = {
        'SÍ': 1, 'SI': 1, '1': 1, '1.0': 1, 'TRUE': 1,
        'NO': 0, '0': 0, '0.0': 0, 'FALSE': 0, 'NAN': 0
    }
    
    df_feedback["Ticket_Soporte"] = soporte_raw.map(mapeo_soporte).fillna(0).astype(int)

    # 7. Cálculo de Calidad Final
    salud_despues = calcular_health_score(df_feedback)

    metricas = {
        "health_score_antes": salud_antes[0],
        "health_score_despues": salud_despues[0],
        "nps_promedio": round(df_feedback["NPS_Numerico"].mean(), 2),
        "rating_mediana": round(valor_relleno_rating, 2),
        "edades_corregidas": edades_corregidas,
        "ratings_corregidos": ratings_corregidos
    }
    cols_texto = df_feedback.select_dtypes(include=['object', 'string']).columns
     
    for col in cols_texto:
        # A. Convertir a minúsculas
        df_feedback[col] = df_feedback[col].str.lower()
        
        # B. Eliminar tildes (Normalización Unicode)
        # Explicación técnica: 
        # 'NFKD' separa la letra de la tilde (á -> a + ´). 
        # 'encode' elimina los caracteres no ASCII (la tilde suelta).
        # 'decode' devuelve el texto a string legible.
        df_feedback[col] = df_feedback[col].str.normalize('NFKD')\
                         .str.encode('ascii', errors='ignore')\
                         .str.decode('utf-8')
    return df_feedback, metricas

def calcular_health_score(df):
    if df.empty: return (0, 0, 0)
    total_celdas = df.size
    total_nulos = df.isna().sum().sum()
    porcentaje_nulos = total_nulos / total_celdas if total_celdas > 0 else 0
    duplicados = df.duplicated().sum()
    porcentaje_duplicados = duplicados / len(df) if len(df) > 0 else 0
    score = 100 * (1 - (0.7 * porcentaje_nulos + 0.3 * porcentaje_duplicados))

    return round(score, 2), round(porcentaje_nulos * 100, 2), round(porcentaje_duplicados * 100, 2)
