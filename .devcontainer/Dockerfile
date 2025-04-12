ARG VARIANT=3.12
FROM mcr.microsoft.com/devcontainers/python:${VARIANT}

# Eviter les soucis ARM avec torch
ARG PYTORCH_VERSION=2.1.0
ARG CUDA_VERSION=cpu

# Libs système utiles
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    gnupg \
    software-properties-common \
    sudo \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer torch et autres libs LLM
RUN pip install --upgrade pip

# Installer Ollama
RUN curl -fsSL https://ollama.com/install.sh | sh

# Expose Ollama's default port
# EXPOSE 11434

# Start Ollama by default
# CMD ["ollama", "serve"]