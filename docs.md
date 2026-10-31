# Cyber Sentinel — Documentação

Monitoramento de sistema com IA: um coletor lê CPU/RAM reais da máquina e envia
por streaming gRPC; o servidor consulta o Ollama (`llama3.1`) e devolve um
diagnóstico. Um painel desktop reúne tudo numa interface única.

## Arquitetura

```
┌─ Windows (nativo) ──────────────────────────────────────┐
│  Ollama (servindo llama3.1)  →  API em 127.0.0.1:11434  │
│                                                         │
│  Painel desktop (app/) ────────────────┐                │
│   ├─ servidor gRPC :50051 (in-process) │                │
│   ├─ coletor (psutil, em thread)       │                │
│   ├─ gráficos CPU/RAM (matplotlib)     │                │
│   └─ feed de diagnósticos (Markdown)   │                │
└─────────────────────────────────────────────────────────┘
```

O painel **embute** o servidor gRPC e o coletor: ao clicar em "Iniciar", ele sobe
o servidor (em processo), inicia a coleta e passa a enviar a telemetria pelo
próprio stream gRPC, exibindo gráficos e diagnósticos.

> Docker foi removido do projeto (adiado para um planejamento futuro). Hoje tudo
> roda nativo.

## Pré-requisitos e execução

1. Python 3.10+ e Ollama instalado (https://ollama.com/download).
2. Baixar o modelo (uma vez): `ollama pull llama3.1`.

Rodar o painel:

```bash
pip install -r requirements-app.txt
python app/main.py
```

Alternativas standalone:

```bash
python server/main.py      # só o servidor gRPC
python collector/client.py # só o coletor
```

Gerar o executável:

```bash
pip install pyinstaller
python build_exe.py        # gera dist/cyber-sentinel.exe
```

## Estrutura do projeto

```
proto/                  contrato gRPC + stubs gerados
server/main.py          servidor gRPC + consulta ao Ollama
collector/client.py     coletor standalone (sem a interface)
app/                    painel desktop (Tkinter)
  main.py               janela + widgets + orquestração
  coletor.py            threads de métricas e cliente gRPC
  charts.py             gráficos CPU/RAM
  markdown.py           renderizador Markdown → tk.Text
  ollama_check.py       checagem e download de modelo
  theme.py              paleta de cores dark
build_exe.py            script PyInstaller (.exe único)
requirements*.txt       dependências (servidor / coletor / app)
```

## Contrato gRPC (`proto/system_monitor.proto`)

- `service SystemMonitor` com dois RPCs:
  - `SendAlert` (unário) — **definido, mas não implementado** no servidor.
  - `StreamTelemetry` (streaming bidirecional) — usado de fato.
- Mensagens: `TelemetryData` (cpu_percent, ram_percent, timestamp) e
  `AnalysisResponse` (diagnosis).

Os arquivos `system_monitor_pb2.py` e `system_monitor_pb2_grpc.py` são **gerados**
— não editar. Regenerar após mudar o `.proto`:

```bash
python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/system_monitor.proto
```

## Servidor (`server/main.py`)

- `criar_servidor(porta=50051)` cria e inicia o servidor de forma **não
  bloqueante** e o retorna (usado pelo painel, que o roda em processo).
- `iniciar_servidor()` é o modo standalone (bloqueia em `wait_for_termination`).
- `StreamTelemetry`: para cada amostra, monta um prompt curto e chama
  `ollama.generate`; a resposta vira o `diagnosis` devolvido ao cliente.
- `_host_ollama()` lê `OLLAMA_HOST` e normaliza para `http://...`.
- `_extrair_texto()` aceita resposta como dict (versões antigas do pacote
  `ollama`) ou objeto (versões novas).
- Erros na consulta à IA são capturados e devolvidos no próprio diagnóstico
  (`[Erro ao consultar a IA: ...]`) — a stream não quebra.

## Coletor (`collector/client.py` e `app/coletor.py`)

- `collector/client.py`: versão standalone; lê `psutil` e envia pelo stream.
- `app/coletor.py`: classe `Coletor` usada pelo painel, com duas threads:
  - **métricas** — lê CPU/RAM a cada intervalo, alimenta o histórico (deque) e
    mantém apenas a amostra mais recente numa fila de envio (`maxsize=1`).
  - **gRPC** — abre `StreamTelemetry` e recebe os diagnósticos numa fila.
- O desacoplamento garante que gráficos sigam suaves mesmo quando a inferência
  (CPU) é lenta.

## Painel desktop (`app/`)

- `main.py` — classe `App` monta a interface: cabeçalho (título + pills de
  status), gráficos à esquerda, feed de diagnósticos à direita, log e rodapé
  (config + botões).
- `Pill` e `PillButton` — widgets próprios desenhados com `tk.Canvas`
  (retângulo arredondado via `create_polygon(smooth=True)`), para o visual
  "pill" sem cantos retos.
- Modelo de threads: as threads de coleta escrevem em filas/deques; a thread
  principal só lê via `root.after(...)`, mantendo a UI responsiva.
- `charts.py` — `matplotlib` (backend `TkAgg`); gráficos sem "caixa" (spines
  removidas), área preenchida sob a linha, título com a % atual
  (`CPU 42.3%`), e redimensionamento ao mudar o tamanho da janela.
- `markdown.py` — renderizador leve (sem dependências) que converte títulos
  (`#`), `**negrito**`, *itálico*, `` `código` `` e listas (`-`, `1.`) em tags
  do `tk.Text`.
- `ollama_check.py` — `verificar()` lista modelos via `Client.list()`;
  `modelo_presente()` e `baixar_modelo()` (equivalente a `ollama pull`).
- `theme.py` — paleta dark centralizada (acento violeta `#7c6cff`).

## Empacotamento (`build_exe.py`)

PyInstaller em modo `--onefile --windowed`, com hidden imports dos stubs
(`system_monitor_pb2*`) e `--collect-all` de `grpc` e `matplotlib`. O app roda o
servidor em processo, então o `.exe` não precisa invocar outro Python.

## Decisões de design

- **Docker adiado**: o build baixava ~5 GB de modelo dentro da imagem e sofria
  com instabilidade do registry do Ollama (503/timeout). Optou-se por execução
  nativa e deixar a conteinerização para depois.
- **Tudo em um app**: o painel embute servidor + coletor para experiência de um
  clique só.
- **Sem comentários**: o código foi limpo de comentários/docstrings por opção do
  autor.

## Pontos em aberto

- `SendAlert` não implementado (retorna `UNIMPLEMENTED`).
- `consumer/` vazio (reservado).
- Reintroduzir Docker (ver plano da "Abordagem A" na memória do agente).
