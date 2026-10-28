# syntax=docker/dockerfile:1
# Base: ollama/ollama (Ubuntu 24.04) já traz o binário do Ollama, suporte CUDA
# e as env NVIDIA_* prontas para GPU. Python vem do apt (3.12 no 24.04).

# ---- Stage 1: builder — regenera os stubs gRPC a partir do .proto ----
FROM ollama/ollama:latest AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages --no-cache-dir grpcio-tools

WORKDIR /build
COPY proto/system_monitor.proto ./proto/
RUN python3 -m grpc_tools.protoc \
        -I proto \
        --python_out=/build/gen \
        --grpc_python_out=/build/gen \
        proto/system_monitor.proto

# ---- Stage 2: runtime — Ollama + modelo + servidor gRPC ----
FROM ollama/ollama:latest AS runtime

COPY requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install --break-system-packages --no-cache-dir -r /tmp/requirements.txt \
    && apt-get purge -y --auto-remove python3-pip \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

# stubs gerados + código da aplicação
COPY --from=builder /build/gen /app/proto
COPY server/main.py /app/server/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app

# Pré-baixa o modelo para dentro da imagem (~4,7 GB), evitando download em runtime
RUN nohup ollama serve > /tmp/ollama-build.log 2>&1 & \
    until ollama list >/dev/null 2>&1; do sleep 1; done \
    && ollama pull llama3.1

ENV OLLAMA_MODEL=llama3.1 \
    OLLAMA_HOST=127.0.0.1:11434

ENTRYPOINT ["/entrypoint.sh"]
