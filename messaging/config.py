import os


RABBITMQ_URL = os.getenv(
    'RABBITMQ_URL', 'amqp://guest:guest@127.0.0.1:5672/%2F'
)
GRPC_TARGET = os.getenv('GRPC_TARGET', '127.0.0.1:50051')
EXCHANGE = os.getenv('RABBITMQ_EXCHANGE', 'cyber-sentinel.events')
QUEUE = os.getenv('RABBITMQ_QUEUE', 'cyber-sentinel.state-machine')
ROUTING_KEY = os.getenv('RABBITMQ_ROUTING_KEY', 'analysis.completed')
DEAD_LETTER_EXCHANGE = os.getenv(
    'RABBITMQ_DEAD_LETTER_EXCHANGE', 'cyber-sentinel.dead-letter'
)
DEAD_LETTER_QUEUE = os.getenv(
    'RABBITMQ_DEAD_LETTER_QUEUE', 'cyber-sentinel.state-machine.dlq'
)
DEAD_LETTER_ROUTING_KEY = os.getenv(
    'RABBITMQ_DEAD_LETTER_ROUTING_KEY', 'state-machine.failed'
)


def caminho_dados(variavel_ambiente, nome_arquivo):
    configurado = os.getenv(variavel_ambiente)
    if configurado:
        caminho = os.path.abspath(os.path.expanduser(configurado))
        pasta = os.path.dirname(caminho)
    else:
        base = os.getenv('LOCALAPPDATA') or os.path.join(
            os.path.expanduser('~'), '.cyber-sentinel'
        )
        pasta = os.path.join(base, 'CyberSentinel')
        caminho = os.path.join(pasta, nome_arquivo)

    os.makedirs(pasta, exist_ok=True)
    return caminho


def declarar_topologia(channel):
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type='topic',
        durable=True,
    )
    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type='direct',
        durable=True,
    )
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(
        queue=DEAD_LETTER_QUEUE,
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )
    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
            'x-dead-letter-routing-key': DEAD_LETTER_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=QUEUE,
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
    )
