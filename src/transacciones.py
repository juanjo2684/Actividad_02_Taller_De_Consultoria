import pandas as pd
import numpy as np
 
# Cargar los datasets
df_inv = pd.read_csv('inventario_central_v2.csv')
df_trx = pd.read_csv('transacciones_logistica_v2.csv')
df_feed = pd.read_csv('feedback_clientes_v2.csv')
 
# 1. LIMPIEZA Y ESTANDARIZACIÓN
# Estandarizar Bodegas (Norte, norte -> Norte)
df_inv['Bodega_Origen'] = df_inv['Bodega_Origen'].str.title().str.strip()
 
# Estandarizar Ciudades (BOG->Bogotá, MED->Medellín)
city_map = {
    'BOG': 'Bogotá', 'Bogotá': 'Bogotá',
    'MED': 'Medellín', 'Medellín': 'Medellín',
    'Cali': 'Cali',
    'Barranquilla': 'Barranquilla',
    'Bucaramanga': 'Bucaramanga',
    'Cartagena': 'Cartagena'
}
# Aplicar mapa si la ciudad está en las llaves, sino dejar original
df_trx['Ciudad_Destino'] = df_trx['Ciudad_Destino'].map(lambda x: city_map.get(x, x))
 
# 2. UNIÓN DE DATOS (JOIN)
# Transacciones + Feedback
df_merged = pd.merge(df_trx, df_feed, on='Transaccion_ID', how='inner')
# + Inventario (para obtener Bodega de origen)
df_full = pd.merge(df_merged, df_inv[['SKU_ID', 'Bodega_Origen']], on='SKU_ID', how='left')
 
# 3. FILTRADO DE DATOS VALIDOS
# Excluir tiempos de entrega "999" (pérdidas) para no distorsionar la correlación de tiempo real vs NPS
# Excluir NPS nulos
df_clean = df_full[
    (df_full['Tiempo_Entrega_Real'] < 900) & 
    (df_full['Tiempo_Entrega_Real'].notnull()) &
    (df_full['Satisfaccion_NPS'].notnull())
].copy()
 
# 4. ANÁLISIS POR RUTA (Bodega -> Ciudad)
# Agrupar por Bodega y Ciudad
grouped = df_clean.groupby(['Bodega_Origen', 'Ciudad_Destino'])
 
results = []
for name, group in grouped:
    if len(group) > 10: # Solo considerar rutas con suficientes datos
        # Correlación entre Tiempo y NPS
        corr = group['Tiempo_Entrega_Real'].corr(group['Satisfaccion_NPS'])
        avg_time = group['Tiempo_Entrega_Real'].mean()
        avg_nps = group['Satisfaccion_NPS'].mean()
        count = len(group)
        results.append({
            'Bodega_Origen': name[0],
            'Ciudad_Destino': name[1],
            'Correlacion_Tiempo_NPS': corr,
            'Tiempo_Promedio_Dias': avg_time,
            'NPS_Promedio': avg_nps,
            'Volumen_Envios': count
        })
 
df_results = pd.DataFrame(results)
 
# Ordenar por correlación más negativa (fuerte vínculo entre demora y odio del cliente)
# y luego por NPS más bajo.
df_crisis = df_results.sort_values(by=['Correlacion_Tiempo_NPS', 'NPS_Promedio'], ascending=[True, True])
 
import ace_tools as tools; tools.display_dataframe_to_user(name="Analisis Logistica Correlacion", dataframe=df_crisis)
