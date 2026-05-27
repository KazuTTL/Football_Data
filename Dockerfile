FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files
COPY . .

# Expose Streamlit port
EXPOSE 8501

ENV PYTHONUNBUFFERED=1
ENV PROJECT_ROOT=/app

# Default command: Run Streamlit dashboard
CMD ["streamlit", "run", "Phase_4/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
