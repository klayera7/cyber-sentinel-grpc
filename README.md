# cyber-sentinel-grpc

Servidor gRPC que recebe telemetria (CPU/RAM/GPU) de um coletor, aplica regras
rápidas para estados comuns e usa Ollama (`llama3.1`) com cache para analisar
estados elevados e devolver diagnósticos.

## Arquitetura

```
┌─ Windows (nativo) ──────────────────────────────────────┐
│  Ollama (servindo o modelo llama3.1)                    │
│   └─ API em 127.0.0.1:11434                             │
│                                                         │
│  Servidor gRPC  :50051  → regras + cache + Ollama       │
│  Coletor        → envia CPU/RAM/GPU/temperatura         │
│  Servidor       → outbox SQLite → RabbitMQ              │
│  consumer/      → máquina de estados independente       │
│  Transições     → SendAlert → toast + sino na interface │
│  Sino           → histórico filtrável da sessão          │
└─────────────────────────────────────────────────────────┘
```

Tudo roda **nativo no Windows**: o Ollama serve o modelo localmente, o servidor
gRPC usa regras instantâneas para estados comuns e consulta o Ollama com cache
para estados elevados. Ao clicar em **Iniciar**, também inicia o consumidor de
estados como processo filho. O coletor usa `psutil` para
ler CPU/RAM reais da máquina e envia via streaming. Uso da GPU, VRAM e
temperatura da GPU vêm de `nvidia-smi` (sem precisar de administrador); se não
houver GPU NVIDIA, esses campos são simplesmente omitidos.

## Pré-requisitos

1. **Python 3.10+** instalado (testado com 3.12).
2. **Ollama** instalado nativamente no Windows: https://ollama.com/download
3. RabbitMQ instalado como serviço Windows (com Erlang). O botão **Iniciar** do
   painel tenta ativar o serviço e aguarda o endpoint `127.0.0.1:5672`; execute
   o painel como administrador para permitir iniciar/parar o serviço.
4. Modelo `llama3.1` baixado (feito uma única vez):

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

## Rodar o consumidor de decisões separadamente

O painel inicia e encerra o consumidor junto com a coleta. No Windows, o
consumidor standalone encerra quando o processo pai do terminal termina. Para
executá-lo em modo independente, com RabbitMQ instalado e ativo:

```bash
pip install -r requirements-consumer.txt
python consumer/main.py
```

Cada amostra coletada publica um evento rápido para a máquina de estados, sem
esperar a análise do Ollama; os eventos de análise não alteram o estado. O
consumidor persiste transições em SQLite e as registra; não executa ações
automáticas no sistema. Ao detectar uma transição, chama `SendAlert`; o painel mostra um popup
e mantém a notificação no sino até ser aberta. Não execute o comando separado ao
mesmo tempo que o painel, pois a fila aceita um consumidor de estados ativo.
O menu do sino também mostra diagnósticos, decisões e logs recebidos enquanto
estava fechado; o histórico só existe durante a sessão atual do app.

Os limites globais de CPU e RAM (padrão 85% para ambos) geram alerta após 2
coletas consecutivas. Ao entrar em estado crítico por CPU/RAM, o painel também
mostra o processo de maior contribuição como possível pista, sem limite
individual por processo. O popup dura 10 segundos e as ações pendentes ficam no
histórico da sessão. Encerrar exige confirmação e revalidação do processo e do
estado global; se o sistema normalizar, a ação é cancelada. Não há encerramento
automático.

## Configuração (env)

| Variável       | Default            | Descrição                       |
| -------------- | ------------------ | ------------------------------- |
| `OLLAMA_MODEL` | `llama3.1`         | Modelo usado pelo agente de IA  |
| `OLLAMA_HOST`  | `127.0.0.1:11434`  | Endereço do Ollama (nativo)     |
| `RABBITMQ_URL` | `amqp://guest:guest@127.0.0.1:5672/%2F` | URL do broker |
| `RABBITMQ_SERVICE_NAME` | `RabbitMQ` | Nome do serviço RabbitMQ no Windows |
| `GRPC_TARGET` | `127.0.0.1:50051` | Destino gRPC do consumidor para enviar alertas |
| `SENTINEL_STATE_DB` | `%LOCALAPPDATA%/CyberSentinel/state_machine.sqlite3` | Banco da máquina de estados |
| `SENTINEL_HOST_ID` | hostname local | Identificador estável opcional da máquina |
| `SENTINEL_OUTBOX_DB` | `%LOCALAPPDATA%/CyberSentinel/rabbit_outbox.sqlite3` | Banco de eventos pendentes |
| `SENTINEL_CPU_LIMIT` | `85` | Limite global de CPU (%) |
| `SENTINEL_RAM_LIMIT` | `85` | Limite global de RAM (%) |
| `SENTINEL_SETTINGS` | `%LOCALAPPDATA%/CyberSentinel/settings.json` | Preferências salvas dos limites |

## Interface gráfica (desktop)

Um painel único que reúne servidor + coletor + visualização numa só janela
(gráficos de CPU/RAM/GPU/temperatura da GPU em tempo real e feed de diagnósticos da IA).

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
- `server/` — servidor gRPC, regras e cache de diagnóstico, integração Ollama
- `collector/` — cliente que envia CPU/RAM via streaming
- `app/` — interface gráfica (Tkinter) com servidor + coletor embutidos
  - `sensores.py` — leitura de GPU/VRAM/temperatura da GPU via `nvidia-smi`
  - `processos.py` — top processos e encerramento com validação
  - `rabbitmq_service.py` — inicialização/verificação do serviço RabbitMQ
  - `historico.py` — histórico filtrável em memória da sessão
- `messaging/` — contrato/configuração RabbitMQ e schema dos eventos
- `consumer/` — consumidor RabbitMQ, máquina de estados e armazenamento SQLite
- `SendAlert` — entrega gRPC de notificações à interface desktop
- `requirements-consumer.txt` — dependências do consumidor
- `build_exe.py` — gera o executável (PyInstaller)
- `requirements.txt` — dependências do servidor
- `requirements-collector.txt` — dependências do coletor
- `requirements-app.txt` — dependências da interface gráfica
