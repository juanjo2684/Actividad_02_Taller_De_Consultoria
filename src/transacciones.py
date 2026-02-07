# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def procesar_transacciones(ruta_csv, df_inventario, df_feedback):

    try:
        df_raw = pd.read_csv(ruta_csv)
    except Exception as e:
        return pd.DataFrame(), {"error": str(e)}

    df_trans = df_raw.copy()

    # 1. Limpieza de nombres de columnas
    df_trans.columns = [c.strip() for c in df_trans.columns]

    # ==========================================
    # PASO 2: CONVERSIÓN DE TIPOS DE DATO
    # ==========================================
    # Convertir Fecha_Venta a datetime para análisis temporal
    df_trans['Fecha_Venta'] = pd.to_datetime(df_trans['Fecha_Venta'])

    
    # ==========================================
    # PASO 4: CONVERSIÓN DE CANTIDAD_VENDIDA A POSITIVO
    # ==========================================
    # Valores negativos son errores de entrada, se convierten a positivos
    df_trans.loc[:, 'Cantidad_Vendida'] = df_trans.loc[:, 'Cantidad_Vendida'].abs()

    # ==========================================
    # PASO 5: IMPUTACIÓN CONDICIONAL DE ESTADO_ENVIO
    # ==========================================
    
    # Estrategia: Usar información de feedback para inferir estado de envío

    # Paso 5a: Transacciones SIN ticket de soporte -> "entregado"
    # (clientes sin problemas, no abrieron ticket)
    transacciones_nps_no = df_feedback[df_feedback['Ticket_Soporte'] == 0]['Transaccion_ID'].unique()

    condicion_existe = df_trans['Transaccion_ID'].isin(transacciones_nps_no)
    condicion_vacio = df_trans['Estado_Envio'].isna()

    df_trans.loc[condicion_existe & condicion_vacio, 'Estado_Envio'] = 'entregado'

    # Paso 5b: Transacciones CON ticket de soporte abierto -> "devuelto"
    # (clientes con problemas, abrieron ticket)
    transacciones_nps_si = df_feedback[df_feedback['Ticket_Soporte'] == 1]['Transaccion_ID'].unique()

    condicion_existe = df_trans['Transaccion_ID'].isin(transacciones_nps_si)
    condicion_vacio = df_trans['Estado_Envio'].isna()

    df_trans.loc[condicion_existe & condicion_vacio, 'Estado_Envio'] = 'devuelto'

    # ==========================================
    # PASO 6: NORMALIZACIÓN DE CIUDADES DESTINO
    # ==========================================
    # Mapeo de abreviaturas a nombres completos
    dic_ciudades = {
            "BOG": "Bogotá", "BOGOTA": "Bogotá",
            "MED": "Medellín", "MEDELLIN": "Medellín",
            "BAQ": "Barranquilla", "BARRANQUILLA": "Barranquilla",
            "VENTAS_WEB": "Canal digital"
        }
    df_trans['Ciudad_Destino'].replace(dic_ciudades, inplace=True)

    # ==========================================
    # PASO 7: IMPUTACIÓN SELECTIVA DE COSTO_ENVIO
    # ==========================================
    # Lógica de negocio: No hay envío en transacciones de canal físico (tienda)
    df_trans.loc[
        df_trans['Canal_Venta'] == 'físico', 
        'Costo_Envio'
    ] = 0

    # ==========================================
    # PASO 8: FEATURE ENGINEERING - MÁRGENES
    # ==========================================
    # Crear métricas de rentabilidad

    # Margen absoluto: Precio_Venta_Final - Costo_Envio
    df_trans['margen'] = (
        df_trans['Precio_Venta_Final'] - df_trans['Costo_Envio']
    )

    # Margen porcentual: (Margen / Precio_Venta_Final) * 100
    df_trans['margen %'] = (
        df_trans['margen'] / df_trans['Precio_Venta_Final']
    )

    # ==========================================
    # PASO 9: ENRIQUECIMIENTO - MERGE CON INVENTARIO
    # ==========================================
    # Traer información de bodega del inventario
    # Left join: Mantener todas las transacciones, agregar bodega si existe
    df_trans = df_trans.merge(
        df_inventario[['SKU_ID', 'Bodega_Origen']],  # Solo columnas necesarias
        on='SKU_ID',                                   # Llave de unión
        how='left'                                     # Left join
    )

    # ==========================================
    # PASO 10: CREACIÓN DE IDENTIFICADOR GRUPAL
    # ==========================================
    # Crear ID único para cada ruta bodega-ciudad
    # Útil para imputación de tiempos y costos por ruta
    df_trans['id_tiempos_entrega'] = (
        df_trans['Bodega_Origen'] + "-" + df_trans['Ciudad_Destino']
    )

    # ==========================================
    # PASO 11: IMPUTACIÓN GRUPAL - TIEMPO_ENTREGA_REAL
    # ==========================================
    # Llenar nulos con la mediana del grupo bodega-ciudad
    # Esto mantiene consistencia de tiempos por ruta

    df_trans.loc[df_trans['Tiempo_Entrega_Real'] == 999, 'Tiempo_Entrega_Real'] = np.nan
    df_trans['Tiempo_Entrega_Real'] = df_trans['Tiempo_Entrega_Real'].fillna(
        df_trans.groupby('id_tiempos_entrega')['Tiempo_Entrega_Real'].transform('median').fillna(0)
    )

    # ==========================================
    # PASO 12: IMPUTACIÓN GRUPAL - COSTO_ENVIO
    # ==========================================
    # Llenar nulos con la mediana del grupo bodega-ciudad
    # Mantiene costos realistas por ruta
    df_trans['Costo_Envio'] = df_trans['Costo_Envio'].fillna(
        df_trans.groupby('id_tiempos_entrega')['Costo_Envio'].transform('median')
    )


    # ==========================================
    # PASO 13: CÁLCULO DE FECHA CALCULADA
    # ==========================================
    # Calcular fecha esperada de entrega
    # Formula: Fecha_Venta + Tiempo_Entrega_Real (en días)
    fecha_max = df_trans.Fecha_Venta.max()

    df_trans['Fecha_Calculada'] = df_trans.apply(
        lambda x: x['Fecha_Venta'] + pd.DateOffset(days=int(x['Tiempo_Entrega_Real'])), 
        axis=1
    )

    # ==========================================
    # PASO 14: IMPUTACIÓN LÓGICA - ESTADO_ENVIO
    # ==========================================
    # Paso 14a: Marcar como "entregado" (entregado)
    # Si la fecha calculada es menor a la fecha máxima del dataset
    # Significa que debería haber llegado ya
    df_trans.loc[
        (df_trans['Fecha_Calculada'] < fecha_max) & (df_trans['Estado_Envio'].isna()),
        'Estado_Envio'
    ] = 'entregado'

    # Paso 14b: Marcar como "en camino"
    # Si la fecha calculada es mayor a la fecha máxima
    # Significa que aún está en tránsito (no debería haber llegado)
    df_trans.loc[
        (df_trans['Fecha_Calculada'] >= fecha_max) & (df_trans['Estado_Envio'].isna()),
        'Estado_Envio'
    ] = 'en camino'

    # ==========================================
    # 15. ESTANDARIZACIÓN DIRECTA
    # ---------------------------------------------------------

    if 'Tiempo_Entrega_Real' in df_trans.columns:
        df_trans = df_trans.rename(columns={'Tiempo_Entrega_Real': 'Tiempo_Entrega'})
    elif 'Tiempo_Entrega' not in df_trans.columns:
        df_trans['Tiempo_Entrega'] = np.nan

    # Métricas de salud
    salud_antes = (100, 0, 0)
    salud_despues = (100, 0, 0)

    skus_sin_inventario = 0
    if "SKU_ID" in df_trans.columns and "SKU_ID" in df_inventario.columns:
        skus_sin_inventario = int((~df_trans["SKU_ID"].isin(df_inventario["SKU_ID"])).sum())

    metricas = {
        "health_score_antes": salud_antes[0],
        "health_score_despues": salud_despues[0],
        "total_transacciones": len(df_trans),
        "skus_sin_inventario": skus_sin_inventario
    }
    
    
  

    return df_trans, metricas






