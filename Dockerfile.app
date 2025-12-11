FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements_app.txt .
RUN pip install --no-cache-dir -r requirements_app.txt

# Copy application code
COPY src/ src/
COPY data/ data/ 
COPY models/ models/
COPY .streamlit/ .streamlit/

# Explicitly copy config if needed, or rely on .streamlit/config.toml being picked up
# COPY .streamlit/config.toml /app/.streamlit/config.toml

# Expose port
EXPOSE 8080

# Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Command to run the application
# Cloud Run expects the container to listen on $PORT (default 8080)
CMD streamlit run src/app/streamlit_app.py --server.port $PORT --server.address 0.0.0.0
