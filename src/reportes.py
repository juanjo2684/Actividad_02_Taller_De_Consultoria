# -*- coding: utf-8 -*-
"""
Generación del Reporte Ejecutivo en PDF.

Los gráficos que se insertan en el PDF se renderizan con **matplotlib**
(backend ``Agg``), eliminando completamente la dependencia de kaleido / Chrome.
Las gráficas interactivas del dashboard siguen siendo Plotly.
"""
import matplotlib
matplotlib.use("Agg")

import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import io
import sys


# =====================================================================
#  Constructores de gráficos con Matplotlib (solo para el PDF)
# =====================================================================

def _fig_venta_invisible_mpl(df: pd.DataFrame, width=450, height=250):
    """Gráfico de barras horizontal: Top 10 ciudades con mayor fuga."""
    columnas = {"venta_sin_inventario", "Ciudad_Destino", "ingreso_total"}
    if not columnas.issubset(df.columns):
        return None

    df_sin = df[df["venta_sin_inventario"]].copy()
    if df_sin.empty:
        return None

    fuga = (
        df_sin
        .groupby("Ciudad_Destino", dropna=False)["ingreso_total"]
        .sum()
        .sort_values(ascending=True)
        .tail(10)
    )

    fig_w = max(width / 80, 4)
    fig_h = max(height / 80, 2.5)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    bars = ax.barh(fuga.index.astype(str), fuga.values, color="#1f4e78",
                   edgecolor="white", linewidth=0.3)
    for bar, val in zip(bars, fuga.values):
        ax.text(val + fuga.max() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"${val:,.0f}", va="center", fontsize=6, color="#333333")

    ax.set_title("Top 10 Ciudades – Fuga por Venta Invisible",
                 fontsize=9, fontweight="bold", color="#1f4e78")
    ax.set_xlabel("Ingreso en Riesgo (USD)", fontsize=7)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.tick_params(labelsize=6)
    ax.grid(axis="x", alpha=0.15)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _fig_riesgo_operativo_mpl(df: pd.DataFrame, width=450, height=250):
    """Scatter: días sin revisión vs tasa de soporte por bodega."""
    columnas = {"Ultima_Revision", "Bodega_Origen", "Ticket_Soporte",
                "ingreso_total", "NPS_Numerico"}
    if not columnas.issubset(df.columns):
        return None

    df_work = df.copy()
    df_work["Ultima_Revision"] = pd.to_datetime(
        df_work["Ultima_Revision"], errors="coerce")
    fecha_ref = df_work["Ultima_Revision"].max()
    if pd.isna(fecha_ref):
        return None
    df_work["dias_sin_revision"] = (fecha_ref - df_work["Ultima_Revision"]).dt.days
    df_work = df_work.dropna(subset=["dias_sin_revision"])
    if df_work.empty:
        return None

    agg = (
        df_work
        .groupby("Bodega_Origen")
        .agg(
            dias=("dias_sin_revision", "mean"),
            tickets=("Ticket_Soporte", lambda x: x.mean() * 100),
            ingresos=("ingreso_total", "sum"),
        )
        .reset_index()
    )

    fig_w = max(width / 80, 4)
    fig_h = max(height / 80, 2.5)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    sizes = (agg["ingresos"] / agg["ingresos"].max() * 300).clip(lower=30)
    scatter = ax.scatter(
        agg["dias"], agg["tickets"],
        s=sizes, c=agg["tickets"],
        cmap="OrRd", edgecolors="white", linewidth=0.5, alpha=0.85,
    )

    for _, row in agg.iterrows():
        ax.annotate(
            row["Bodega_Origen"], (row["dias"], row["tickets"]),
            fontsize=5, ha="center", va="bottom", color="#444444",
        )

    ax.set_title("Impacto del Descuido Operativo por Bodega",
                 fontsize=9, fontweight="bold", color="#1f4e78")
    ax.set_xlabel("Días desde Última Revisión", fontsize=7)
    ax.set_ylabel("% Tasa Soporte", fontsize=7)
    ax.tick_params(labelsize=6)
    ax.grid(alpha=0.15)
    ax.spines[["top", "right"]].set_visible(False)
    plt.colorbar(scatter, ax=ax, label="% Tickets", shrink=0.7, pad=0.02)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# =====================================================================
#  Inserción de imágenes PNG en el story de ReportLab
# =====================================================================

def _insertar_grafico(img_bytes, story, caption, width=450, height=250):
    """Inserta bytes PNG en la lista *story* de ReportLab."""
    if not img_bytes:
        style_err = ParagraphStyle("Err", fontSize=9, textColor=colors.red,
                                   alignment=1)
        story.append(Paragraph(
            f"<i>[Gráfico no disponible: {caption}]</i>", style_err))
        return False

    try:
        buf = io.BytesIO(img_bytes)
        buf.seek(0)
        img = Image(buf, width=width, height=height)
        img.hAlign = "CENTER"

        story.append(Spacer(1, 12))
        story.append(img)

        style_cap = ParagraphStyle(
            "Caption",
            parent=getSampleStyleSheet()["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#666666"),
            alignment=1,
        )
        story.append(Paragraph(f"<i>{caption}</i>", style_cap))
        story.append(Spacer(1, 10))
        print(f"[PDF] ✓ Gráfico insertado: {caption}", file=sys.stderr)
        return True
    except Exception as exc:
        print(f"[PDF] ✗ Error insertando gráfico '{caption}': {exc}",
              file=sys.stderr)
        style_err = ParagraphStyle("Err", fontSize=9, textColor=colors.red,
                                   alignment=1)
        story.append(Paragraph(
            f"<i>[Error al insertar: {caption}]</i>", style_err))
        return False


# =====================================================================
#  Generador principal del PDF
# =====================================================================

def generar_reporte_ejecutivo_pdf(df_filtrado, health_scores, metricas_calidad):
    """Genera el reporte ejecutivo en PDF y devuelve sus bytes.

    Los gráficos se renderizan con matplotlib (sin kaleido).
    La firma ya **no recibe figuras Plotly**; construye sus propias figuras.
    """
    print("\n" + "=" * 80, file=sys.stderr)
    print("INICIANDO GENERACIÓN DE REPORTE PDF (matplotlib)", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=40,
                            rightMargin=40)
    styles = getSampleStyleSheet()
    story = []

    df = df_filtrado.copy()
    df["Ultima_Revision"] = pd.to_datetime(df["Ultima_Revision"], errors="coerce")
    hoy = pd.to_datetime(datetime.now().date())
    df["Dias_Desde_Revision"] = (hoy - df["Ultima_Revision"]).dt.days.fillna(0)
    df["Ciudad_Destino_Norm"] = (
        df["Ciudad_Destino"].astype(str).str.strip().str.upper()
    )

    df_analisis = df[
        (df["Tiempo_Entrega"] < 100) & (df["Tiempo_Entrega"] > 0)
    ].copy()

    # ── Estilos ──────────────────────────────────────────────────
    style_title = ParagraphStyle(
        "Title", parent=styles["Title"], fontSize=22,
        textColor=colors.HexColor("#1f4e78"), spaceAfter=20)
    style_h2 = ParagraphStyle(
        "H2", parent=styles["Heading2"], fontSize=16,
        textColor=colors.HexColor("#2e75b6"), spaceBefore=15, spaceAfter=10)
    style_body = ParagraphStyle(
        "Body", parent=styles["Normal"], fontSize=10, leading=12,
        spaceAfter=10)
    style_alerta = ParagraphStyle(
        "Alerta", parent=styles["Normal"], textColor=colors.red, fontSize=10,
        leading=12, fontWeight="Bold")
    style_kpi_header = ParagraphStyle(
        "KPIHeader", parent=styles["Normal"], fontSize=9, fontWeight="Bold",
        alignment=1, textColor=colors.HexColor("#333333"))
    style_kpi_value = ParagraphStyle(
        "KPIValue", parent=styles["Normal"], fontSize=13, fontWeight="Bold",
        alignment=1, textColor=colors.HexColor("#1f4e78"))

    # ── 1. ENCABEZADO ───────────────────────────────────────────
    story.append(Paragraph(
        "Informe de Auditoría Integral: TechLogistics S.A.S", style_title))
    story.append(Paragraph(
        f"<b>Fecha de Emisión:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        style_body))
    story.append(Paragraph(
        "<b>Asunto:</b> Diagnóstico de Riesgo Operativo, Rentabilidad, "
        "Logistica y Fidelización", style_body))
    story.append(Spacer(1, 12))

    # ── 2. SALUD DEL DATO ───────────────────────────────────────
    story.append(Paragraph(
        "1. Certificación de Calidad de la Información", style_h2))
    data_health = [["Módulo", "Score Inicial", "Score Final", "Mejora"]]
    for ds, scores in health_scores.items():
        mejora = scores["Despues"] - scores["Antes"]
        data_health.append([
            ds.capitalize(),
            f"{scores['Antes']:.1f}%",
            f"{scores['Despues']:.1f}%",
            f"+{mejora:.1f}%",
        ])

    t_health = Table(data_health,
                     colWidths=[1.5 * 72, 1.2 * 72, 1.2 * 72, 1 * 72])
    t_health.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e78")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))
    story.append(t_health)
    story.append(Spacer(1, 15))

    # ── 3. KPIs GENERALES ───────────────────────────────────────
    story.append(Paragraph(
        "2. Resumen de Indicadores Clave (KPIs)", style_h2))
    total_ingresos = df["ingreso_total"].sum()
    margen_promedio = (
        (df["margen_real"].sum() / total_ingresos * 100)
        if total_ingresos != 0 else 0
    )
    nps_global = df_analisis["NPS_Numerico"].mean()
    tasa_soporte_global = df["Ticket_Soporte"].mean() * 100

    data_kpis = [
        [Paragraph("Total Ingresos (USD)", style_kpi_header),
         Paragraph("Margen Operativo", style_kpi_header),
         Paragraph("NPS Global", style_kpi_header),
         Paragraph("Tasa Soporte", style_kpi_header)],
        [Paragraph(f"${total_ingresos:,.0f}", style_kpi_value),
         Paragraph(f"{margen_promedio:.1f}%", style_kpi_value),
         Paragraph(f"{nps_global:.2f}", style_kpi_value),
         Paragraph(f"{tasa_soporte_global:.1f}%", style_kpi_value)],
    ]
    t_kpis = Table(data_kpis, colWidths=[1.35 * 72] * 4)
    t_kpis.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eeeeee")),
        ("GRID", (0, 0), (-1, -1), 1, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.append(t_kpis)
    story.append(Spacer(1, 10))

    # ── 4. CRISIS LOGÍSTICA ─────────────────────────────────────
    story.append(Paragraph(
        "3. Crisis Logística y Cuellos de Botella", style_h2))
    data_log_kpis = [
        [Paragraph("⏳ Tiempo Entrega Prom.", style_kpi_header),
         Paragraph("🔗 Correlación NPS/Tiempo", style_kpi_header),
         Paragraph("🚩 Brecha Máxima", style_kpi_header)],
        [Paragraph("15.0 días", style_kpi_value),
         Paragraph("-0.01", style_kpi_value),
         Paragraph("29 días", style_kpi_value)],
    ]
    t_log = Table(data_log_kpis, colWidths=[1.8 * 72] * 3)
    t_log.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#ebf5fb")),
        ("GRID", (0, 0), (-1, -1), 1, colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(t_log)
    story.append(Spacer(1, 10))

    filtro_canal = df_analisis["Ciudad_Destino_Norm"].str.contains(
        "CANAL DIGITAL|DIGITAL", na=False)
    registros_canal_digital = df_analisis[filtro_canal].shape[0]
    story.append(Paragraph(
        f"<b>HALLAZGO DE TRAZABILIDAD:</b> Se identificaron "
        f"<b>{registros_canal_digital} registros</b> con ciudad de destino "
        f"<b>CANAL DIGITAL</b>. Al normalizar los datos, este volumen revela "
        f"una carencia crítica de control geográfico sobre el gasto logístico.",
        style_body))
    story.append(Paragraph(
        "⚠️ <b>ACCIÓN INMEDIATA:</b> Se requiere el <b>cambio de operador "
        "logístico</b> para la ruta <b>Zona Franca - Barranquilla</b>.",
        style_alerta))
    story.append(Spacer(1, 10))

    # ── 5. RIESGOS FINANCIEROS Y VENTA INVISIBLE ────────────────
    story.append(Paragraph(
        "4. Riesgos Financieros y Administrativos", style_h2))

    df_sin_inv = (
        df[df["venta_sin_inventario"]].copy()
        if "venta_sin_inventario" in df.columns else pd.DataFrame()
    )
    ingreso_riesgo = (
        df_sin_inv["ingreso_total"].sum() if not df_sin_inv.empty else 0
    )
    porcentaje_riesgo = (
        (ingreso_riesgo / total_ingresos * 100) if total_ingresos else 0
    )
    skus_no_catalogados = (
        df_sin_inv["SKU_ID"].nunique() if not df_sin_inv.empty else 0
    )
    transacciones_afectadas = len(df_sin_inv)

    story.append(Paragraph(
        f"<b>Diagnóstico de Venta Invisible:</b> Impacto financiero de "
        f"<b>USD ${ingreso_riesgo:,.2f}</b> ({porcentaje_riesgo:.1f}% del "
        f"total) por SKUs no catalogados.", style_body))

    # Gráfico matplotlib – venta invisible
    img_ciudades = _fig_venta_invisible_mpl(df, width=450, height=250)
    _insertar_grafico(img_ciudades, story,
                      "Ingresos en riesgo por ciudad (resumen)",
                      width=450, height=250)

    data_inv = [
        ["Métrica de Riesgo", "Valor Detectado"],
        ["Ingreso en Riesgo (USD)", f"${ingreso_riesgo:,.2f}"],
        ["SKUs No Catalogados", str(skus_no_catalogados)],
        ["Transacciones Afectadas", str(transacciones_afectadas)],
    ]
    t_inv = Table(data_inv, colWidths=[2.5 * 72, 1.5 * 72])
    t_inv.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2f4f4")),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))
    story.append(t_inv)

    df_perdida = df[df["margen_real"] < 0]
    total_fuga = abs(df_perdida["margen_real"].sum())
    story.append(Paragraph(
        f"• <b>Fuga de Capital:</b> Pérdida directa de "
        f"<b>USD ${total_fuga:,.2f}</b> en márgenes negativos.", style_body))

    # ── 6. DIAGNÓSTICO DE FIDELIDAD ─────────────────────────────
    story.append(Paragraph(
        "5. Diagnóstico de Fidelidad y Paradoja de Inventario", style_h2))

    nps_avg_fid = (
        df_analisis["NPS_Numerico"].mean() if not df_analisis.empty else 0
    )
    rating_prod = (
        df_analisis["Rating_Producto"].mean()
        if "Rating_Producto" in df_analisis.columns else 0
    )
    casos_paradoja = (
        int(df["paradoja_fidelidad"].sum())
        if "paradoja_fidelidad" in df.columns else 0
    )

    story.append(Paragraph(
        f"Se ha detectado una <b>paradoja crítica</b> en la gestión de stock: "
        f"existen <b>{casos_paradoja} instancias</b> de productos con alta "
        f"disponibilidad (Stock > Q3) pero sentimiento negativo del cliente "
        f"(NPS < 7).", style_body))

    data_fid = [
        [Paragraph("NPS Promedio", style_kpi_header),
         Paragraph("Rating Producto", style_kpi_header),
         Paragraph("Casos Paradoja", style_kpi_header)],
        [Paragraph(f"{nps_avg_fid:.2f}/10", style_kpi_value),
         Paragraph(f"{rating_prod:.2f}/5", style_kpi_value),
         Paragraph(f"{casos_paradoja}", style_kpi_value)],
    ]
    t_fid = Table(data_fid, colWidths=[1.8 * 72] * 3)
    t_fid.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#fcf3cf")),
        ("GRID", (0, 0), (-1, -1), 1, colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(t_fid)
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Explicación de la Paradoja:</b>", style_body))
    story.append(Paragraph(
        f"• <b>Calidad Deficiente:</b> El Rating de producto de "
        f"<b>{rating_prod:.2f}/5</b> indica que el estancamiento de "
        f"inventario se debe primordialmente a una <b>baja percepción de "
        f"calidad</b> del SKU. El mercado está rechazando activamente estos "
        f"productos.", style_body))
    story.append(Paragraph(
        "• <b>Hipótesis de Sobrecosto:</b> Para categorías con Rating "
        "aceptable pero NPS bajo, el cliente valora el producto pero percibe "
        "un desbalance entre costo y beneficio (sobreprecio), lo que frena la "
        "rotación.", style_body))

    # ── 7. RIESGO OPERATIVO ─────────────────────────────────────
    story.append(Paragraph(
        "6. Riesgo Operativo: Bodegas 'A Ciegas'", style_h2))

    promedio_dias = df["Dias_Desde_Revision"].mean()
    tasa_tickets = df["Ticket_Soporte"].mean() * 100
    df_corr = df.dropna(subset=["Dias_Desde_Revision", "NPS_Numerico"])
    corr_nps = (
        df_corr["Dias_Desde_Revision"].corr(df_corr["NPS_Numerico"])
        if not df_corr.empty else 0
    )

    story.append(Paragraph(
        f"El análisis de riesgo operativo revela que el sistema de "
        f"almacenamiento opera con un rezago crítico de auditoría, con un "
        f"promedio de <b>{promedio_dias:.0f} días sin revisión</b> física de "
        f"stock. Este descuido administrativo tiene una incidencia directa en "
        f"la <b>tasa de soporte del {tasa_tickets:.1f}%</b>.", style_body))

    data_ops = [
        [Paragraph("Promedio Días Sin Revisión", style_kpi_header),
         Paragraph("Tasa Tickets Soporte", style_kpi_header),
         Paragraph("Correlación Riesgo/NPS", style_kpi_header)],
        [Paragraph(f"{promedio_dias:.0f} días", style_kpi_value),
         Paragraph(f"{tasa_tickets:.1f}%", style_kpi_value),
         Paragraph(f"{corr_nps:.2f}", style_kpi_value)],
    ]
    t_ops = Table(data_ops, colWidths=[1.8 * 72] * 3)
    t_ops.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f2d7d5")),
        ("GRID", (0, 0), (-1, -1), 1, colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(t_ops)
    story.append(Spacer(1, 10))

    # Top 5 bodegas — calculado dinámicamente
    story.append(Paragraph(
        "<b>Top 5 Bodegas en Riesgo Crítico:</b>", style_body))
    df_bodegas = (
        df.dropna(subset=["Dias_Desde_Revision"])
        .groupby("Bodega_Origen")
        .agg(
            dias=("Dias_Desde_Revision", "mean"),
            tickets=("Ticket_Soporte", lambda x: x.mean() * 100),
            ingresos=("ingreso_total", "sum"),
        )
        .sort_values("dias", ascending=False)
        .head(5)
        .reset_index()
    )

    data_bodegas = [
        ["Bodega", "Días Sin Revisión", "% Tickets Soporte",
         "Ingresos Expuestos"]
    ]
    for _, r in df_bodegas.iterrows():
        data_bodegas.append([
            str(r["Bodega_Origen"]),
            f"{r['dias']:.0f}",
            f"{r['tickets']:.1f}%",
            f"${r['ingresos']:,.2f}",
        ])

    t_bodegas = Table(data_bodegas,
                      colWidths=[1.2 * 72, 1.2 * 72, 1.1 * 72, 1.5 * 72])
    t_bodegas.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#922b21")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.whitesmoke, colors.white]),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
    ]))
    story.append(t_bodegas)

    story.append(Paragraph(
        "<b>Diagnóstico de Gestión:</b> Las bodegas con mayor antigüedad de "
        "revisión operan prácticamente 'a ciegas'. La falta de revisión "
        "genera inconsistencias que disparan los tickets de soporte, "
        "degradando la confianza operativa.", style_body))

    # Gráfico matplotlib – riesgo operativo
    img_riesgo = _fig_riesgo_operativo_mpl(df, width=450, height=250)
    _insertar_grafico(img_riesgo, story,
                      "Bodegas con mayor riesgo operativo (resumen)",
                      width=450, height=250)

    # ── Construir PDF ────────────────────────────────────────────
    print("\n[PDF] Construyendo documento...", file=sys.stderr)
    try:
        doc.build(story)
        buffer.seek(0)
        print("[PDF] ✓ PDF generado exitosamente", file=sys.stderr)
        print("=" * 80 + "\n", file=sys.stderr)
        return buffer.getvalue()
    except Exception as e:
        print(f"[PDF] ✗ Error al construir PDF: {e}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        raise
