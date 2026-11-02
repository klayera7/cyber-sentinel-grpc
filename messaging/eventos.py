import os
import socket
import uuid
from datetime import datetime, timezone


SCHEMA_VERSION = 1
TIPO_EVENTO = 'system.analysis.completed'


def _opcional(dados, campo):
    return getattr(dados, campo) if dados.HasField(campo) else None


def criar_evento(dados, diagnostico, origem):
    ocorrido_em = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    return {
        'schema_version': SCHEMA_VERSION,
        'event_type': TIPO_EVENTO,
        'event_id': str(uuid.uuid4()),
        'host_id': os.getenv('SENTINEL_HOST_ID') or socket.gethostname(),
        'occurred_at': ocorrido_em.replace('+00:00', 'Z'),
        'sample_timestamp': dados.timestamp,
        'metrics': {
            'cpu_percent': float(dados.cpu_percent),
            'ram_percent': float(dados.ram_percent),
            'gpu_percent': _opcional(dados, 'gpu_percent'),
            'gpu_mem_percent': _opcional(dados, 'gpu_mem_percent'),
            'gpu_temp_celsius': _opcional(dados, 'temp_celsius'),
        },
        'diagnosis': diagnostico,
        'diagnosis_source': origem,
    }
