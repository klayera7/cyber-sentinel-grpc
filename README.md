# cyber-sentinel-grpc

Servidor gRPC que recebe telemetria (CPU/RAM) de um coletor e usa um agente de IA
(Ollama + `llama3.1`) para analisar o estado do sistema e devolver um diagnóstico.

## Arquitetura

```
┌─ Docker Desktop (WSL2) ───────────────────────────┐
│  container "sentinel"                             │
│   ├─ ollama serve  (GPU NVIDIA)                   │
│   ├─ modelo llama3.1                              │
│   └─ servidor gRPC  :50051                        │
└───────────────────────────────────────────────────┘
        ▲ localhost:50051
┌─ Windows (nativo) ────────────┐
│  collector/client.py (psutil) │ → lê CPU/RAM reais do Windows
└───────────────────────────────┘
```

O Ollama e o modelo ficam **dentro** da imagem (imagem única autocontida, ~6 GB).
O coletor roda **fora** do container para conseguir ler as métricas reais da máquina
Windows (dentro do container ele leria apenas a VM WSL2).

## Pré-requisitos

1. **Docker Desktop** com backend **WSL2** — o Docker Desktop instala e configura o
   WSL2 sozinho; **não** é preciso instalar WSL manualmente. Confirme que
   "Use the WSL 2 based engine" está marcado.
2. **Driver NVIDIA** instalado no Windows.
3. **GPU ativada no Docker Desktop** (Settings → Resources → WSL Integration / GPU).

> O Ollama nativo no Windows **não é necessário** — o container traz o próprio Ollama.
> Se você tiver um instalado, pode manter ou desinstalar; não há conflito (só a porta
> `50051` é exposta ao host).

## Rodar o servidor (Ollama + gRPC)

```bash
docker compose up --build
```

A primeira subida baixa o modelo `llama3.1` (~4,7 GB) durante o build e demora.
Depois disso, sobe rápido.

⚠️ Sem driver NVIDIA/GPU configurado no Docker Desktop, o daemon cai em **CPU** e a
inferência fica bem mais lenta (ainda funciona).

## Rodar o coletor (nativo no Windows)

```bash
pip install -r requirements-collector.txt
python collector/client.py
```

## Configuração (env)

| Variável       | Default            | Descrição                           |
| -------------- | ------------------ | ----------------------------------- |
| `OLLAMA_MODEL` | `llama3.1`         | Modelo usado pelo agente de IA      |
| `OLLAMA_HOST`  | `127.0.0.1:11434`  | Endereço do Ollama (interno ao container) |

## Estrutura

- `proto/` — contrato gRPC (`system_monitor.proto`) + stubs gerados
- `server/` — servidor gRPC que consulta a IA (Ollama)
- `collector/` — cliente que envia CPU/RAM via streaming
- `Dockerfile` — imagem multistage (builder regenera stubs; runtime só o necessário)
- `docker-compose.yml` — orquestra o container com reserva de GPU NVIDIA
- `entrypoint.sh` — sobe `ollama serve` + o servidor gRPC no mesmo container
