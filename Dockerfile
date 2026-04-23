FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos "" appuser && \
    mkdir -p /app/data && chown appuser:appuser /app/data

USER appuser
ENV PATH="/home/appuser/.local/bin:${PATH}"

COPY requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
