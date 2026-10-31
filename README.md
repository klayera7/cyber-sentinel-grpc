# cyber-sentinel-grpc

Servidor gRPC que recebe telemetria (CPU/RAM) de um coletor e usa um agente de IA
(Ollama + `llama3.1`) para analisar o estado do sistema e devolver um diagnóstico.

## Arquitetura

```
┌─ Windows (nativo) ──────────────────────────────────────┐
│  Ollama (servindo o modelo llama3.1)                    │
│   └─ API em 127.0.0.1:11434                             │
│                                                         │
│  Servidor gRPC  :50051  → consulta o Ollama             │
│  Coletor        → envia CPU/RAM via streaming           │
└─────────────────────────────────────────────────────────┘
```

Tudo roda **nativo no Windows**: o Ollama serve o modelo localmente, o servidor
gRPC consulta o Ollama para gerar o diagnóstico, e o coletor usa `psutil` para
ler CPU/RAM reais da máquina e envia via streaming.

## Pré-requisitos

1. **Python 3.10+** instalado (testado com 3.12).
2. **Ollama** instalado nativamente no Windows: https://ollama.com/download
3. Modelo `llama3.1` baixado (feito uma única vez):

```bash
ollama pull llama3.1
```

> Sem GPU, o Ollama roda em CPU e a inferência fica mais lenta (ainda funciona).

## Rodar o servidor (Ollama + gRPC)

```bash
# 1. Garanta que o Ollama está rodando e com o modelo disponível.
#    O app do Ollama já deixa o servidor ativo; se necessário, rode:
ollama serve

# 2. Instale as dependências e suba o servidor gRPC:
pip install -r requirements.txt
python server/main.py
```

O servidor sobe na porta `50051`.

## Rodar o coletor

```bash
pip install -r requirements-collector.txt
python collector/client.py
```

## Configuração (env)

| Variável       | Default            | Descrição                       |
| -------------- | ------------------ | ------------------------------- |
| `OLLAMA_MODEL` | `llama3.1`         | Modelo usado pelo agente de IA  |
| `OLLAMA_HOST`  | `127.0.0.1:11434`  | Endereço do Ollama (nativo)     |

## Interface gráfica (desktop)

Um painel único que reúne servidor + coletor + visualização numa só janela
(gráficos de CPU/RAM em tempo real e feed de diagnósticos da IA).

```bash
pip install -r requirements-app.txt
python app/main.py
```

Para gerar o executável único:

```bash
pip install pyinstaller
python build_exe.py      # gera dist/cyber-sentinel.exe
```

## Estrutura

- `proto/` — contrato gRPC (`system_monitor.proto`) + stubs gerados
- `server/` — servidor gRPC que consulta a IA (Ollama)
- `collector/` — cliente que envia CPU/RAM via streaming
- `app/` — interface gráfica (Tkinter) com servidor + coletor embutidos
- `consumer/` — reservado (vazio)
- `build_exe.py` — gera o executável (PyInstaller)
- `requirements.txt` — dependências do servidor
- `requirements-collector.txt` — dependências do coletor
- `requirements-app.txt` — dependências da interface gráfica
