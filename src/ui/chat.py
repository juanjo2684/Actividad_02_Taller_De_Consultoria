# -*- coding: utf-8 -*-
"""
Módulo de chat con agente IA (Groq) para el DSS de TechLogistics.

El usuario inserta su API key de Groq en la barra lateral y puede
conversar con un agente que tiene contexto completo de los datos
cargados en el dashboard.
"""
import streamlit as st
import pandas as pd
import numpy as np


# =====================================================================
#  Helpers para construir el contexto de datos
# =====================================================================

def _resumen_dataframe(df: pd.DataFrame) -> str:
    """Genera un resumen estadístico compacto del dataframe filtrado."""
    lines: list[str] = []
    lines.append(f"Registros: {len(df):,}  |  Columnas: {df.shape[1]}")

    # KPIs financieros
    if "ingreso_total" in df.columns:
        lines.append(f"Ingresos totales: ${df['ingreso_total'].sum():,.2f}")
    if "margen_real" in df.columns:
        margen = df["margen_real"].sum()
        pct = (margen / df["ingreso_total"].sum() * 100) if df["ingreso_total"].sum() else 0
        lines.append(f"Margen neto: ${margen:,.2f} ({pct:.1f}%)")
        perdidas = df[df["margen_real"] < 0]
        lines.append(f"Transacciones con pérdida: {len(perdidas):,} (fuga ${abs(perdidas['margen_real'].sum()):,.2f})")

    # Venta invisible
    if "venta_sin_inventario" in df.columns:
        vi = df["venta_sin_inventario"].sum()
        lines.append(f"Ventas sin inventario (venta invisible): {vi:,} ({vi / len(df) * 100:.1f}%)")

    # NPS
    if "NPS_Numerico" in df.columns:
        lines.append(f"NPS promedio: {df['NPS_Numerico'].mean():.2f}")
    if "NPS_Categoria" in df.columns:
        dist = df["NPS_Categoria"].value_counts().to_dict()
        lines.append(f"Distribución NPS: {dist}")

    # Soporte
    if "Ticket_Soporte" in df.columns:
        tasa = df["Ticket_Soporte"].mean() * 100
        lines.append(f"Tasa de tickets de soporte: {tasa:.1f}%")

    # Logística
    if "Tiempo_Entrega" in df.columns:
        te = df["Tiempo_Entrega"]
        lines.append(f"Tiempo entrega (media/mediana): {te.mean():.1f} / {te.median():.1f} días")
    if "brecha_entrega" in df.columns:
        lines.append(f"Brecha logística media: {df['brecha_entrega'].mean():.1f} días")

    # Inventario
    if "Stock_Actual" in df.columns:
        lines.append(f"Stock actual (media): {df['Stock_Actual'].mean():.0f}")
    if "paradoja_fidelidad" in df.columns:
        lines.append(f"Casos paradoja fidelidad: {int(df['paradoja_fidelidad'].sum()):,}")

    # Categorías y ciudades
    if "Categoria" in df.columns:
        top_cat = df.groupby("Categoria")["ingreso_total"].sum().nlargest(5)
        lines.append("Top 5 categorías por ingreso:")
        for cat, val in top_cat.items():
            lines.append(f"  - {cat}: ${val:,.0f}")

    if "Ciudad_Destino" in df.columns:
        top_city = df.groupby("Ciudad_Destino")["ingreso_total"].sum().nlargest(5)
        lines.append("Top 5 ciudades por ingreso:")
        for city, val in top_city.items():
            lines.append(f"  - {city}: ${val:,.0f}")

    # Bodegas y riesgo operativo
    if "Bodega_Origen" in df.columns and "Ultima_Revision" in df.columns:
        df_temp = df.copy()
        df_temp["Ultima_Revision"] = pd.to_datetime(df_temp["Ultima_Revision"], errors="coerce")
        ref = df_temp["Ultima_Revision"].max()
        if pd.notna(ref):
            df_temp["_dias"] = (ref - df_temp["Ultima_Revision"]).dt.days
            bod = (
                df_temp.groupby("Bodega_Origen")
                .agg(dias=("_dias", "mean"), ing=("ingreso_total", "sum"))
                .nlargest(5, "dias")
            )
            lines.append("Top 5 bodegas con mayor antigüedad de revisión:")
            for idx, r in bod.iterrows():
                lines.append(f"  - {idx}: {r['dias']:.0f} días, ${r['ing']:,.0f}")

    # Rating
    if "Rating_Producto" in df.columns:
        lines.append(f"Rating producto promedio: {df['Rating_Producto'].mean():.2f}/5")

    return "\n".join(lines)


def _build_system_prompt(df: pd.DataFrame, health_scores: dict) -> str:
    """Construye el system prompt con el contexto de datos."""
    resumen = _resumen_dataframe(df)

    # Health scores
    hs_lines = []
    for ds, sc in health_scores.items():
        hs_lines.append(f"  {ds}: {sc['Antes']:.1f}% → {sc['Despues']:.1f}%")
    hs_text = "\n".join(hs_lines)

    # Columnas disponibles
    cols = ", ".join(sorted(df.columns.tolist()))

    return f"""Eres un analista de datos experto especializado en logística, \
operaciones y cadena de suministro. Trabajas como consultor para \
TechLogistics S.A.S, una empresa de logística colombiana.

Tu rol es responder preguntas sobre los datos del dashboard de auditoría \
que el usuario está visualizando. Responde siempre en español, sé conciso \
y apóyate en los números.

== CONTEXTO DEL DATASET FILTRADO ==
{resumen}

== HEALTH SCORES (CALIDAD DE DATOS) ==
{hs_text}

== COLUMNAS DISPONIBLES ==
{cols}

Reglas:
- Si el usuario pregunta algo que no se puede responder con los datos, dilo.
- Proporciona cifras concretas cuando sea posible.
- Si detectas hallazgos críticos, menciónalos proactivamente.
- Puedes sugerir acciones de mejora basadas en los datos.
- Usa formato markdown para tablas y listas cuando sea útil.
"""


# =====================================================================
#  Inicialización del estado de sesión
# =====================================================================

def _init_chat_state() -> None:
    if "groq_api_key" not in st.session_state:
        st.session_state.groq_api_key = ""
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "groq_model" not in st.session_state:
        st.session_state.groq_model = "llama-3.3-70b-versatile"


# =====================================================================
#  Componente principal del chat
# =====================================================================

def render_chat_sidebar_config() -> None:
    """Renderiza solo la configuración del chat en la barra lateral."""
    _init_chat_state()

    st.sidebar.markdown("---")
    st.sidebar.subheader("🤖 Asistente IA (Groq)")

    api_key = st.sidebar.text_input(
        "🔑 API Key de Groq",
        type="password",
        value=st.session_state.groq_api_key,
        placeholder="gsk_...",
        help="Obtén tu key gratis en https://console.groq.com",
    )
    st.session_state.groq_api_key = api_key

    modelo = st.sidebar.selectbox(
        "Modelo",
        options=[
            "llama-3.3-70b-versatile",
            "llama-3.1-8b-instant",
            "mixtral-8x7b-32768",
            "gemma2-9b-it",
        ],
        index=0,
        help="Selecciona el modelo de lenguaje a utilizar",
    )
    st.session_state.groq_model = modelo

    if api_key:
        st.sidebar.success("✅ API Key configurada")
    else:
        st.sidebar.info("Ingresa tu API Key de Groq para habilitar el chat")

    if st.sidebar.button("🗑️ Limpiar conversación"):
        st.session_state.chat_messages = []
        st.rerun()


def render_chat_panel(df_filtrado: pd.DataFrame, health_scores: dict) -> None:
    """Renderiza el panel de chat en el contenedor donde se invoque (lado derecho)."""

    _init_chat_state()
    api_key = st.session_state.groq_api_key

    st.markdown(
        """
        <div style="
            background: #ffffff;
            border: 1px solid #e3eaf3;
            border-radius: 14px;
            padding: 1.2rem;
            box-shadow: 0 8px 20px rgba(15,23,42,0.06);
        ">
            <h3 style="margin-top:0; color:#1f4e78;">🤖 Asistente IA</h3>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not api_key:
        st.info("🔑 Ingresa tu API Key de Groq en la barra lateral para habilitar el chat.")
        return

    # Intentar importar groq
    try:
        from groq import Groq
    except ImportError:
        st.error(
            "❌ La librería `groq` no está instalada. "
            "Ejecuta: `pip install groq`"
        )
        return

    # Contenedor con scroll para el historial
    chat_container = st.container(height=480)
    with chat_container:
        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # Input del usuario
    prompt = st.chat_input(
        "Pregúntale al asistente sobre los datos...",
        key="groq_chat_input",
    )

    if prompt:
        st.session_state.chat_messages.append(
            {"role": "user", "content": prompt}
        )

        with chat_container:
            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                with st.spinner("Pensando..."):
                    try:
                        client = Groq(api_key=api_key)

                        system_prompt = _build_system_prompt(
                            df_filtrado, health_scores
                        )

                        messages = [{"role": "system", "content": system_prompt}]
                        messages.extend(
                            st.session_state.chat_messages[-20:]
                        )

                        response = client.chat.completions.create(
                            model=st.session_state.groq_model,
                            messages=messages,
                            temperature=0.3,
                            max_tokens=2048,
                        )

                        reply = response.choices[0].message.content
                        st.markdown(reply)

                        st.session_state.chat_messages.append(
                            {"role": "assistant", "content": reply}
                        )

                    except Exception as e:
                        error_msg = f"❌ Error al comunicarse con Groq: {e}"
                        st.error(error_msg)
                        st.session_state.chat_messages.append(
                            {"role": "assistant", "content": error_msg}
                        )


def render_chat_section(df_filtrado: pd.DataFrame, health_scores: dict) -> None:
    """Compatibilidad: renderiza sidebar config + panel de chat a la derecha."""
    render_chat_sidebar_config()
    render_chat_panel(df_filtrado, health_scores)
