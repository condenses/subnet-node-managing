# syntax=docker/dockerfile:1
FROM python:3.10-slim

# Prevent Python from writing pyc files to disc and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Copy project configuration files and source code
COPY pyproject.toml .
COPY README.md .
COPY condenses_node_managing ./condenses_node_managing

# Upgrade pip, install Hatch build tool, and install the project as a package.
RUN pip install --upgrade pip && \
    pip install uv && \
    uv venv && . .venv/bin/activate && \
    uv sync --prerelease=allow

ENV PATH=/app/.venv/bin:$PATH

# Expose the port specified in the settings (default 9100)
EXPOSE 9101

# Command to run the server using the entrypoint defined in pyproject.toml
CMD ["condenses-node-managing-start-server"] 