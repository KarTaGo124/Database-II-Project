# Dockerfile para Database-II-Project
# Sistema de gesti�n de base de datos con m�ltiples �ndices y GUI web

# Usar Python 3.13 como imagen base
FROM python:3.13-slim

# Establecer directorio de trabajo
WORKDIR /app

# Informaci�n del mantenedor
LABEL maintainer="Database-II-Project Team"
LABEL description="Database Management System with Multiple Indexing Structures"

# Instalar dependencias del sistema necesarias para rtree (libspatialindex)
# y otras herramientas útiles
RUN apt-get update && apt-get install -y --no-install-recommends \
    libspatialindex-dev \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivo de requisitos primero para aprovechar el cache de Docker
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar todo el c�digo del proyecto
COPY . .

# Crear directorios necesarios si no existen
RUN mkdir -p data/datasets && \
    mkdir -p experiments/results && \
    mkdir -p tests

# Exponer el puerto de Streamlit (puerto por defecto: 8501)
EXPOSE 8501

# Variables de entorno para Streamlit
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Healthcheck para verificar que Streamlit est� corriendo
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Comando por defecto: ejecutar la GUI de Streamlit
CMD ["streamlit", "run", "gui/main.py", "--server.port=8501", "--server.address=0.0.0.0"]
