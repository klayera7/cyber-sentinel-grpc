# Cyber Sentinel — Documentação

Monitoramento de sistema: um coletor lê CPU/RAM/GPU e envia por streaming gRPC;
o servidor combina regras rápidas e Ollama (`llama3.1`) para gerar diagnósticos,
que também são publicados no RabbitMQ para uma máquina de estados independente.

## Arquitetura

```
┌─ Windows (nativo) ──────────────────────────────────────┐
│  Ollama (servindo llama3.1)  →  API em 127.0.0.1:11434  │
│                                                         │
│  Painel desktop (app/) ────────────────┐                │
│   ├─ servidor gRPC :50051 (in-process) │                │
│   ├─ coletor (psutil + nvidia-smi)     │                │
│   ├─ gráficos CPU/RAM/GPU/temperatura  │                │
│   └─ outbox SQLite → RabbitMQ          │                │
│                                        │                │
│  Processo consumer/ → máquina de estados                │
└─────────────────────────────────────────────────────────┘
```

O painel **embute** o servidor gRPC e o coletor: ao clicar em "Iniciar", ele sobe
o servidor (em processo), inicia a coleta e passa a enviar a telemetria pelo
próprio stream gRPC, exibindo gráficos e diagnósticos.

Após cada amostra processada, o servidor também grava um evento no outbox SQLite.
Um worker publica os eventos em RabbitMQ; o processo independente `consumer/`
consome a fila, persiste o estado e registra transições. Ao clicar em **Iniciar**
no painel, `consumer/main.py` é iniciado como processo filho e encerrado junto
com a sessão de monitoramento.

> Docker foi removido do projeto (adiado para um planejamento futuro). Hoje tudo
> roda nativo.

## Pré-requisitos e execução

1. Python 3.10+, Ollama instalado (https://ollama.com/download) e RabbitMQ
   instalado como serviço Windows (requer Erlang).
2. Baixar o modelo (uma vez): `ollama pull llama3.1`.
3. O botão **Iniciar** tenta iniciar o serviço RabbitMQ e aguardar o endpoint
   AMQP local (`127.0.0.1:5672`) antes de iniciar gRPC/coletor. Ao clicar em
   **Parar** ou fechar o painel, o gRPC e o publisher são encerrados primeiro e
   depois o serviço RabbitMQ é parado. O Windows pode exigir permissão para
   iniciar/parar o serviço; execute o painel com permissões administrativas.

   Para impedir que RabbitMQ inicie com o Windows, configure o serviço uma vez
   em CMD como administrador: `sc config RabbitMQ start= demand`.

Rodar o painel:

```bash
pip install -r requirements-app.txt
python app/main.py
```

Alternativas standalone:

```bash
python server/main.py      # só o servidor gRPC
python collector/client.py # só o coletor
python consumer/main.py    # consumidor independente (alternativa ao processo gerenciado pelo painel)
```

O consumidor pode ser instalado em um ambiente separado com
`pip install -r requirements-consumer.txt` (inclui `psutil`). No Windows, ele
encerra quando o processo pai do terminal termina; para execução destacada,
mantenha um processo pai vivo. Para o painel, `grpcio`, `protobuf` e `pika` já
estão em `requirements-app.txt`. Não execute uma segunda instância do
consumidor enquanto o painel estiver ativo.

## RabbitMQ e máquina de estados

- O produtor publica um evento JSON `system.analysis.completed` para cada
  amostra coletada no painel, com CPU/RAM, GPU/VRAM/temperatura GPU (nulos se
  não houver sensor), diagnóstico por regra, `event_id`, `host_id`, timestamps
  e `state_relevant: true`. A análise do Ollama também gera um evento, mas com
  `state_relevant: false`, para não atrasar nem duplicar a máquina de estados.
  A deduplicação do feed da interface não descarta eventos.

  ```json
  {
    "schema_version": 1,
    "event_type": "system.analysis.completed",
    "event_id": "uuid",
    "host_id": "nome-da-maquina",
    "occurred_at": "2026-11-02T12:00:00.000Z",
    "sample_timestamp": "12:00:00",
    "metrics": {
      "cpu_percent": 42.0,
      "ram_percent": 67.0,
      "gpu_percent": 20.0,
      "gpu_mem_percent": 12.0,
      "gpu_temp_celsius": 62.0
    },
    "diagnosis": "Sistema estável...",
    "diagnosis_source": "regra"
  }
  ```
- Exchange topic durável `cyber-sentinel.events`, routing key
  `analysis.completed` e fila durável `cyber-sentinel.state-machine`. Mensagens
  são persistentes e usam publisher confirms.
- `server/outbox.py` grava primeiro no SQLite local; `server/rabbit_publisher.py`
  publica em uma thread separada com reconexão e backoff. Com RabbitMQ fora do ar,
  o gRPC continua funcionando e os eventos ficam no outbox. No painel, o outbox é
  por sessão e não é reenviado depois do encerramento; no servidor standalone sem
  sessão, ele tenta novamente na próxima execução. O banco fica em
  `%LOCALAPPDATA%\\CyberSentinel` por padrão.
- `consumer/main.py` usa um consumidor exclusivo, `ack` manual e prefetch 1 para
  preservar a ordem. Eventos inválidos ou que falham após três tentativas são
  enviados à fila DLQ
  `cyber-sentinel.state-machine.dlq`.
- `consumer/state_machine.py` usa `NORMAL`, `ATENÇÃO` e `CRÍTICO`. CPU e RAM usam
  um limite configurável cada; atingir qualquer um deles por 2 coletas
  consecutivas gera `CRÍTICO`. GPU/VRAM continuam em atenção a partir de 70%/80%
  e crítico em 90%; temperatura da GPU usa 75 °C/90 °C. A recuperação exige 5
  coletas consecutivas abaixo dos limites.
- Cada amostra do coletor publica um evento de avaliação imediato, independente
  da análise do Ollama, para que amostras descartadas pela fila de envio não
  atrasem os alertas. Eventos de análise são marcados com
  `state_relevant: false` e não alteram a máquina de estados.
- `consumer/state_store.py` persiste estados, eventos processados e transições
  em SQLite. O consumidor confirma a mensagem após a transação; `event_id`
  torna reentregas idempotentes.
- `app/processos.py` coleta os principais processos de CPU/RAM com PID, nome,
  horário de criação e memória RSS. Quando uma transição global para `CRÍTICO`
  é causada por CPU/RAM, o evento inclui o processo de maior contribuição como
  possível pista; não há limite individual por processo.
- A sugestão de encerramento aparece no popup por 10 segundos e também fica nas
  ações pendentes do histórico da sessão. A confirmação mostra todos os PIDs do
  mesmo caminho completo do executável e seus descendentes. Cada raiz é encerrada
  com a árvore do Windows. O launcher Steam não é agrupado só por ter iniciado
  o jogo. A ação revalida PID, `create_time` e caminho, e só prossegue se o
  sistema ainda estiver crítico e CPU/RAM exceder um limite configurado. Se o
  sistema normalizar, a sugestão expira; não há ação automática.
- Em cada transição, o consumidor mantém uma notificação pendente no SQLite e
  chama `SendAlert` no gRPC. Só marca a notificação como enviada após receber
  confirmação. Estado e outbox são separados por sessão; mensagens RabbitMQ com
  outro `session_id` (incluindo mensagens antigas sem ID) são confirmadas e
  descartadas sem alterar o estado da sessão atual.
- `SendAlert` entrega alertas para a fila thread-safe da interface. O painel
  mostra um toast e mantém o alerta no sino/histórico mesmo se o toast for
  ignorado. Abrir o sino marca os alertas como lidos.
- O menu **Histórico da sessão** consulta um SQLite `:memory:` com diagnósticos,
  decisões, notificações e logs operacionais. Ele continua recebendo registros
  com a janela fechada, oferece filtros/pesquisa/paginação e é descartado quando
  o app termina; não guarda amostras brutas de telemetria.
- Configuração: `RABBITMQ_URL`, `GRPC_TARGET`, `SENTINEL_HOST_ID`,
  `SENTINEL_OUTBOX_DB`, `SENTINEL_STATE_DB`, `SENTINEL_CPU_LIMIT` e
  `SENTINEL_RAM_LIMIT`; `RABBITMQ_SERVICE_NAME` altera o nome do serviço
  Windows (padrão `RabbitMQ`). Os dois limites globais são editáveis no painel e
  persistidos em `settings.json` em `%LOCALAPPDATA%\\CyberSentinel` (ou em
  `SENTINEL_SETTINGS`). O padrão usa credenciais `guest` apenas para o broker
  local; para acesso remoto, configure usuário dedicado e TLS. Se o serviço
  estiver ausente ou sem permissão, o painel avisa e continua a coleta usando o
  outbox da sessão; as pendências não passam para a sessão seguinte.

Gerar o executável:

```bash
pip install pyinstaller
python build_exe.py        # gera dist/cyber-sentinel.exe
```

## Estrutura do projeto

```
proto/                  contrato gRPC + stubs gerados
server/main.py          servidor gRPC + consulta ao Ollama
server/diagnostico.py   regras rápidas + cache LRU/TTL dos diagnósticos
server/rabbit_publisher.py publisher assíncrono com confirmações
server/outbox.py        outbox durável SQLite
messaging/              schema/topologia RabbitMQ + log local de depuração
consumer/main.py        consumidor RabbitMQ (processo independente)
consumer/state_machine.py estados e histerese das transições
consumer/state_store.py estado, idempotência e transições em SQLite
collector/client.py     coletor standalone (sem a interface)
app/                    painel desktop (Tkinter)
  main.py               janela + widgets + orquestração
  config_limites.py     limites configuráveis persistidos em JSON
  historico.py          log de sessão filtrável, somente em memória
  coletor.py            threads de métricas e cliente gRPC
  processos.py          top processos e encerramento com validação de identidade
  sensores.py           leitura de GPU/temperatura via nvidia-smi
  rabbitmq_service.py  inicia/verifica serviço RabbitMQ no botão Iniciar
  charts.py             gráficos CPU/RAM/GPU/temperatura
  markdown.py           renderizador Markdown → tk.Text
  ollama_check.py       checagem e download de modelo
  theme.py              paleta de cores dark
build_exe.py            script PyInstaller (.exe único)
requirements*.txt       dependências (servidor / coletor / app)
```

## Log de depuração

O painel, o servidor, o publicador RabbitMQ e o consumidor acrescentam eventos a
`logs/debug.txt` na pasta do projeto. O arquivo é local e ignorado pelo Git; não é
o histórico temporário da interface. Cada linha inclui horário local com fuso e
horário UTC, além do processo/thread e dados do evento. São registrados amostras,
diagnósticos, contagens/transições de estado, notificações, confirmações e
falhas/retries do outbox. URLs com credenciais e
campos de segredo são redigidos. `SENTINEL_DEBUG_LOG` pode apontar para outro
caminho; no executável, o padrão é `logs/debug.txt` ao lado do `.exe`.

Cada clique em **Iniciar** cria um UUID de sessão. O outbox e o estado SQLite da
sessão ficam em `%LOCALAPPDATA%\CyberSentinel\sessions\<session_id>` (ou sob a
pasta configurada por `SENTINEL_OUTBOX_DB` / `SENTINEL_STATE_DB`). Os eventos
carregam o ID; o consumidor confirma e descarta eventos de outras sessões, então
mensagens antigas na fila não geram diagnósticos, decisões ou popups novos.
Eventos que ficarem no outbox de uma sessão encerrada não são reenviados depois;
o fechamento registra a pendência em vez de prometer retomada.
Também é criado `logs/sessoes/<session_id>.md`, atualizado durante a execução com
uma linha do tempo legível, decisões e notificações, métricas por amostra e um
resumo min/média/máx ao encerrar. Se o processo cair, o relatório parcial é
marcado como interrompido quando uma sessão posterior começar.

## Contrato gRPC (`proto/system_monitor.proto`)

- `service SystemMonitor` com dois RPCs:
  - `SendAlert` (unário) — recebe alertas (`INFO`, `WARNING`, `CRITICAL`),
    entrega à UI e responde com `AlertAck`; deduplica por `alert_id`.
  - `StreamTelemetry` (streaming bidirecional) — usado de fato.
- Mensagens: `TelemetryData` (CPU/RAM/GPU e `top_processes`), `ProcessTelemetry`
  (PID, nome, `create_time`, CPU/RAM e RSS), `AnalysisResponse` (diagnóstico),
  `AlertRequest` (nível, mensagem e `ProcessRecommendation` opcional para a
  sugestão de encerramento) e `AlertAck`. Os
  campos opcionais de sensor distinguem "sem sensor" de "valor zero" via
  `HasField`.

Os arquivos `system_monitor_pb2.py` e `system_monitor_pb2_grpc.py` são **gerados**
— não editar. Regenerar após mudar o `.proto`:

```bash
python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/system_monitor.proto
```

## Servidor (`server/main.py`)

- `criar_servidor(porta=50051)` cria e inicia o servidor de forma **não
  bloqueante**, inicia o worker RabbitMQ e o retorna (usado pelo painel, que o
  roda em processo). `parar_servidor()` encerra gRPC e drena o outbox por até
  três segundos; no modo painel, o que restar fica na pasta da sessão e não é
  reenviado. No servidor standalone sem sessão, o outbox continua disponível
  para a próxima execução.
- `iniciar_servidor()` é o modo standalone (bloqueia em `wait_for_termination`).
- `StreamTelemetry`: a telemetria continua sendo recebida em todas as amostras.
  Estados normais recebem diagnóstico determinístico imediato; estados elevados
  consultam `ollama.generate` quando não há resposta em cache. Em cache miss, o
  prompt recebe os valores reais de CPU, RAM, GPU/VRAM e temperatura da GPU.
- `server/diagnostico.py`: agrupa chaves em faixas de 10% para CPU/RAM/GPU/VRAM
  e 5 °C para a temperatura da GPU. O cache LRU mantém até 100 respostas por
  10 minutos; chamadas à IA têm cooldown de 4 segundos e são limitadas a uma
  inferência simultânea. Os diagnósticos determinísticos cobrem também estados
  elevados quando o cooldown impede uma chamada.
- O servidor registra a origem (`IA`, `cache` ou `regra`). Falhas da IA são
  registradas no servidor e produzem o diagnóstico determinístico como fallback;
  a stream gRPC continua ativa.
- `_host_ollama()` lê `OLLAMA_HOST` e normaliza para `http://...`.
- `_extrair_texto()` aceita resposta como dict (versões antigas do pacote
  `ollama`) ou objeto (versões novas).
- `app/main.py` suprime cards consecutivos com o mesmo texto para evitar que
  estados estáveis encham o feed com diagnósticos repetidos.

## Coletor (`collector/client.py` e `app/coletor.py`)

- `collector/client.py`: versão standalone; lê `psutil` e envia pelo stream
  (apenas CPU/RAM — não consulta os sensores de GPU/temperatura).
- `app/coletor.py`: classe `Coletor` usada pelo painel, com duas threads:
  - **métricas** — lê CPU/RAM (psutil) e GPU/temperatura (`app/sensores.py`),
    monta uma `Amostra` (dataclass), alimenta o histórico (deque) e mantém
    apenas a amostra mais recente numa fila de envio (`maxsize=1`).
  - **gRPC** — abre `StreamTelemetry` e recebe os diagnósticos numa fila.
- O desacoplamento garante que gráficos sigam suaves mesmo quando a inferência
  (CPU) é lenta.

## Sensores (`app/sensores.py`)

- A API não expõe a GPU; a leitura vem de uma única chamada por ciclo:

  ```bash
  nvidia-smi --query-gpu=temperature.gpu,utilization.gpu,memory.used,memory.total \
             --format=csv,noheader,nounits
  ```

- Devolve `LeituraGPU(uso, memoria, temperatura)`; a VRAM é convertida em %.
- A temperatura exibida e enviada ao servidor é especificamente a da GPU
  (`temperature.gpu`); não representa a temperatura de todo o computador.
- Qualquer falha (sem GPU, driver ausente, `nvidia-smi` com erro) vira `None`,
  e a UI mostra `—` em vez de quebrar. O subprocess roda com
  `CREATE_NO_WINDOW` para não abrir console no `.exe`.

## Painel desktop (`app/`)

- `main.py` — classe `App` monta a interface: cabeçalho (título + pills de
  status de serviço), gráficos à esquerda, painel à direita com pills de GPU e
  temperatura + feed de diagnósticos, log e rodapé (config + botões).
- `Pill` e `PillButton` — widgets próprios desenhados com `tk.Canvas`
  (retângulo arredondado via `create_polygon(smooth=True)`), para o visual
  "pill" sem cantos retos.
- Modelo de threads: as threads de coleta escrevem em filas/deques; a thread
  principal só lê via `root.after(...)`, mantendo a UI responsiva.
- `charts.py` — `matplotlib` (backend `TkAgg`) em grade 2×2 (CPU, RAM, GPU,
  temperatura da GPU); gráficos sem "caixa" (spines removidas), área preenchida
  sob a linha e título com o valor atual. O backend TkAgg controla o
  redimensionamento, inclusive em tela cheia.
  - **RAM** e **temperatura da GPU** usam escala vertical adaptada ao histórico,
    com faixa mínima para tornar variações visíveis; CPU e GPU continuam em
    0–100%.
  - **GPU**: duas linhas no mesmo eixo 0–100 (uso sólido, VRAM tracejada) com
    legenda mínima.
  - **Temperatura da GPU**: o título mostra `GPU TEMP 62.0 °C · 42% térmica`,
    onde o percentual é a *carga térmica* normalizada entre 35 °C e 100 °C
    (`(T - 35) / 65`, limitada a 0–100). A cor segue as faixas: verde < 60 °C,
    amarelo 60–75 °C, vermelho ≥ 75 °C.
  - Amostras sem sensor viram `NaN`: o traço fica interrompido em vez de cair
    para zero.
- `markdown.py` — renderizador leve (sem dependências) que converte títulos
  (`#`), `**negrito**`, *itálico*, `` `código` `` e listas (`-`, `1.`) em tags
  do `tk.Text`.
- `ollama_check.py` — `verificar()` lista modelos via `Client.list()`;
  `modelo_presente()` e `baixar_modelo()` (equivalente a `ollama pull`).
- `theme.py` — paleta dark centralizada (acento violeta `#7c6cff`).

## Empacotamento (`build_exe.py`)

PyInstaller em modo `--onefile --windowed`, com hidden imports dos stubs
(`system_monitor_pb2*`) e `--collect-all` de `grpc`, `pika` e `matplotlib`. O app roda o
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

- Temperatura de CPU: hoje só a GPU é lida (`nvidia-smi`). Para incluir a CPU é
  preciso LibreHardwareMonitor + `pythonnet` (exige execução como
  administrador e empacotar a DLL no `.exe`).
- Reintroduzir Docker (ver plano da "Abordagem A" na memória do agente).
