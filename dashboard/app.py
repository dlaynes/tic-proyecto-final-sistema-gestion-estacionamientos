import streamlit as st
import pandas as pd
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import queue
import requests
import os
import io
import html
import re
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, HRFlowable
)

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(page_title="Smart Parking", page_icon="🅿️", layout="wide")

# --- CONFIGURACIÓN MQTT ---
MQTT_BROKER = "localhost"      
MQTT_PORT = 1883
PARKING_ID = "estacionamiento-central"
MQTT_TOPIC = f"iot/{PARKING_ID}/espacio/+/estado"
TOTAL_SPOTS = 8
# --- CONFIGURACIÓN N8N ---
N8N_ANALYSIS_WEBHOOK = os.getenv(
    "N8N_ANALYSIS_WEBHOOK",
    "https://primary-production-35ad554b.up.railway.app/webhook/parking-analysis"
)

# --- COLA GLOBAL (Fuera del session_state para evitar el error de Threads) ---
# Esta cola recibe los mensajes del hilo de MQTT
mqtt_queue = queue.Queue()

# --- ESTADO DE LA SESIÓN ---
if 'parking_data' not in st.session_state:
    st.session_state.parking_data = {f"A-{i+1:02d}": False for i in range(TOTAL_SPOTS)}
if 'history' not in st.session_state:
    st.session_state.history = []
if 'mqtt_connected' not in st.session_state:
    st.session_state.mqtt_connected = False
if 'last_update' not in st.session_state:
    st.session_state.last_update = "Sin datos"
if 'pdf_report' not in st.session_state:
    st.session_state.pdf_report = None

def update_state():
    occupied = sum(st.session_state.parking_data.values())
    st.session_state.history.append({
        "Hora": datetime.now().strftime("%H:%M:%S"),
        "Ocupados": occupied
    })
    if len(st.session_state.history) > 20:
        st.session_state.history.pop(0)

# --- CALLBACK DE MQTT (Corre en segundo plano) ---
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        # Guardamos el mensaje en la cola global (NO en session_state)
        mqtt_queue.put(payload)
    except Exception as e:
        print(f"Error procesando mensaje MQTT: {e}")

# --- CLIENTE MQTT ---
@st.cache_resource
def init_mqtt():
    # Usamos CallbackAPIVersion.VERSION2 para quitar el DeprecationWarning
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.subscribe(MQTT_TOPIC)
        client.loop_start()  
        return client
    except Exception as e:
        print(f"No se pudo conectar a MQTT: {e}")
        return None

if 'mqtt_client' not in st.session_state:
    st.session_state.mqtt_client = init_mqtt()
    if st.session_state.mqtt_client:
        st.session_state.mqtt_connected = True


# --- DASHBOARD CON ACTUALIZACIÓN AUTOMÁTICA ---
def generar_pdf(informe):
    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=1.8 * cm,
        leftMargin=1.8 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm
    )

    azul = colors.HexColor("#3D348B")
    azul_claro = colors.HexColor("#E0E1FF")
    gris = colors.HexColor("#F3F4F6")
    texto = colors.HexColor("#333333")

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="Portada",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=21,
        leading=27,
        textColor=azul,
        alignment=TA_CENTER,
        spaceAfter=8
    ))

    styles.add(ParagraphStyle(
        name="Subtitulo",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#777777"),
        alignment=TA_CENTER,
        leading=15
    ))

    styles.add(ParagraphStyle(
        name="Seccion",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=17,
        textColor=azul,
        spaceBefore=15,
        spaceAfter=9
    ))

    styles.add(ParagraphStyle(
        name="Contenido",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=15,
        textColor=texto,
        spaceAfter=7,
        alignment=4
    ))

    styles.add(ParagraphStyle(
        name="Celda",
        parent=styles["BodyText"],
        fontSize=8.5,
        leading=12,
        textColor=texto
    ))

    styles.add(ParagraphStyle(
        name="CeldaBlanca",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        leading=12,
        textColor=colors.white
    ))

    contenido = []

    # Encabezado del informe
    contenido.append(Spacer(1, 15))
    contenido.append(Paragraph(
        "INFORME ADMINISTRATIVO",
        styles["Portada"]
    ))
    contenido.append(Paragraph(
        "SISTEMA INTELIGENTE DE GESTIÓN DE ESTACIONAMIENTOS",
        styles["Subtitulo"]
    ))
    contenido.append(Spacer(1, 12))
    contenido.append(HRFlowable(
        width="100%",
        thickness=2,
        color=azul,
        spaceAfter=18
    ))

    # Corregir fechas con formato YYYYnMMnDD
    informe = re.sub(
        r"(\d{4})n(\d{2})n(\d{2})",
        r"\1-\2-\3",
        informe
    )

    lineas = informe.split("\n")
    i = 0

    while i < len(lineas):
        linea = lineas[i].strip()

        if not linea or linea == "---":
            i += 1
            continue

        # Detectar tablas Markdown
        if linea.startswith("|") and i + 1 < len(lineas):
            filas = []

            while i < len(lineas) and lineas[i].strip().startswith("|"):
                fila = lineas[i].strip()

                # Ignorar separador de tabla
                if not re.match(r"^\|[\s\-:|]+\|$", fila):
                    celdas = [
                        celda.strip()
                        for celda in fila.strip("|").split("|")
                    ]
                    filas.append(celdas)

                i += 1

            if filas:
                datos = []

                for indice, fila in enumerate(filas):
                    estilo = (
                        styles["CeldaBlanca"]
                        if indice == 0
                        else styles["Celda"]
                    )

                    datos.append([
                        Paragraph(html.escape(c), estilo)
                        for c in fila
                    ])

                tabla = Table(
                    datos,
                    repeatRows=1,
                    colWidths=[
                        doc.width * 0.65,
                        doc.width * 0.35
                    ] if len(filas[0]) == 2 else None
                )

                tabla.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), azul),
                    ("BACKGROUND", (0, 1), (-1, -1), gris),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.white),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 9),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]))

                contenido.append(Spacer(1, 5))
                contenido.append(tabla)
                contenido.append(Spacer(1, 12))

            continue

        # Títulos y secciones
        if linea.startswith("#"):
            titulo = linea.lstrip("#").strip()
            titulo = html.escape(titulo)

            contenido.append(Paragraph(
                titulo,
                styles["Seccion"]
            ))

        # Listas
        elif linea.startswith("- ") or linea.startswith("* "):
            texto_lista = linea[2:].strip()
            texto_lista = html.escape(texto_lista)
            texto_lista = re.sub(
                r"\*\*(.*?)\*\*",
                r"<b>\1</b>",
                texto_lista
            )

            contenido.append(Paragraph(
                "• " + texto_lista,
                styles["Contenido"]
            ))

        # Numeración
        elif re.match(r"^\d+\.\s", linea):
            texto_lista = re.sub(r"^\d+\.\s*", "", linea)
            texto_lista = html.escape(texto_lista)
            texto_lista = re.sub(
                r"\*\*(.*?)\*\*",
                r"<b>\1</b>",
                texto_lista
            )

            numero = re.match(r"^(\d+)\.", linea).group(1)

            contenido.append(Paragraph(
                f"<b>{numero}.</b> {texto_lista}",
                styles["Contenido"]
            ))

        # Texto normal
        else:
            texto_linea = html.escape(linea)
            texto_linea = re.sub(
                r"\*\*(.*?)\*\*",
                r"<b>\1</b>",
                texto_linea
            )

            contenido.append(Paragraph(
                texto_linea,
                styles["Contenido"]
            ))

        i += 1

    # Pie de página
    def pie_pagina(canvas, documento):
        canvas.saveState()
        canvas.setStrokeColor(azul_claro)
        canvas.line(
            1.8 * cm, 1.3 * cm,
            A4[0] - 1.8 * cm, 1.3 * cm
        )
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.grey)
        canvas.drawString(
            1.8 * cm, 0.9 * cm,
            "Sistema Inteligente de Gestión de Estacionamientos"
        )
        canvas.drawRightString(
            A4[0] - 1.8 * cm, 0.9 * cm,
            f"Página {documento.page}"
        )
        canvas.restoreState()

    doc.build(
        contenido,
        onFirstPage=pie_pagina,
        onLaterPages=pie_pagina
    )

    buffer.seek(0)
    return buffer.getvalue()

@st.fragment(run_every="2s")
def render_dashboard():

    # 1. Procesar mensajes recibidos por MQTT
    updated = False

    while not mqtt_queue.empty():
        payload = mqtt_queue.get()

        spot_id = payload.get("spot_id")
        occupied = payload.get("occupied", False)
        timestamp = payload.get("timestamp", "Actualizado")

        if spot_id in st.session_state.parking_data:
            # Actualizar la última recepción, aunque no cambie la ocupación
            st.session_state.last_update = timestamp

            # Actualizar el estado del espacio
            if st.session_state.parking_data[spot_id] != occupied:
                st.session_state.parking_data[spot_id] = occupied
                updated = True

    # Registrar en el historial solo los cambios de ocupación
    if updated:
        update_state()

    # 2. Calcular métricas
    occupied_count = sum(st.session_state.parking_data.values())
    total_spots = len(st.session_state.parking_data)

    # 3. Encabezado
    st.title("🅿️ Panel de aparcamiento inteligente")

    if st.session_state.mqtt_connected:
        st.success(
            f"🟢 Conectado a MQTT - Última actualización: "
            f"{st.session_state.last_update}"
        )
    else:
        st.warning("🟡 Modo Simulación - Sin conexión MQTT.")

    if occupied_count == total_spots:
        st.error("🔴 ESTACIONAMIENTO LLENO - Alerta enviada por n8n")

    st.divider()

    # 4. Distribución del dashboard
    col_left, col_right = st.columns([1.2, 1])

    with col_left:
        st.subheader("🗺️ Mapa de Espacios")

        spot_ids = list(st.session_state.parking_data.keys())

        for row in range(2):
            cols = st.columns(4)

            for col_idx in range(4):
                spot_index = row * 4 + col_idx

                if spot_index < len(spot_ids):
                    spot_id = spot_ids[spot_index]
                    is_occupied = st.session_state.parking_data[spot_id]

                    with cols[col_idx]:
                        if is_occupied:
                            if st.button(
                                f"🚗 {spot_id}",
                                key=f"spot_{spot_id}",
                                use_container_width=True,
                                type="primary"
                            ):
                                st.session_state.parking_data[spot_id] = False
                                update_state()
                                st.rerun()
                        else:
                            if st.button(
                                f"🟢 {spot_id}",
                                key=f"spot_{spot_id}",
                                use_container_width=True
                            ):
                                st.session_state.parking_data[spot_id] = True
                                update_state()
                                st.rerun()

        st.divider()

        st.subheader("📊 Estado Actual")

        c1, c2, c3, c4 = st.columns(4)

        c1.metric("Total", total_spots)
        c2.metric("Libres", total_spots - occupied_count)
        c3.metric("Ocupados", occupied_count)
        c4.metric(
            "Ocupación",
            f"{(occupied_count / total_spots) * 100:.0f}%"
        )

    with col_right:
        st.subheader("📈 Historial de Ocupación")

        if st.session_state.history:
            df = pd.DataFrame(st.session_state.history)
            st.line_chart(
                df.set_index("Hora")["Ocupados"],
                color="#f56565"
            )
        else:
            st.caption("Aún no hay datos.")

        

        st.divider()
        st.subheader("📄 Informe administrativo")

        if st.button("🤖 Generar informe por IA", use_container_width=True):
            with st.spinner("Generando informe PDF con IA..."):
                try:
                    response = requests.post(
                        N8N_ANALYSIS_WEBHOOK,
                        json={"solicitud": "generar_analisis"},
                        timeout=120
                    )

                    response.raise_for_status()
                    resultado = response.json()

                    informe = resultado.get("informe") or resultado.get("output")

                    if informe:
                        pdf = generar_pdf(informe)
                        st.session_state.pdf_report = pdf
                    else:
                        st.error("No se recibió el informe de IA.")

                except requests.exceptions.Timeout:
                    st.error("El análisis excedió el tiempo de espera.")

                except requests.exceptions.RequestException as e:
                    st.error(f"Error al comunicarse con n8n: {e}")

                except Exception as e:
                    st.error(f"Error al generar el PDF: {e}")

        # Botón de descarga fuera del botón de generación
        if st.session_state.pdf_report:
            st.download_button(
                label="⬇️ Descargar informe PDF",
                data=st.session_state.pdf_report,
                file_name="Informe_Estacionamiento.pdf",
                mime="application/pdf",
                use_container_width=True
            )

# Ejecutar dashboard
render_dashboard()
