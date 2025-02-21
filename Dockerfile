FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml .
COPY README.md .
COPY condenses_node_managing ./condenses_node_managing
RUN pip install --upgrade pip && \
    pip install uv && \
    uv venv && . .venv/bin/activate && \
    uv sync --prerelease=allow

ENV PATH=/app/.venv/bin:$PATH
CMD ["uvicorn", "condenses_node_managing.server:app", "--host", "0.0.0.0", "--port", "8000"]