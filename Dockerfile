FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Stdlib only -- no pip install needed. requirements.txt is intentionally
# empty; kept for tooling that expects the file to exist.

RUN useradd --create-home appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python3", "run_demo.py"]
