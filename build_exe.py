import os

import PyInstaller.__main__

RAIZ = os.path.dirname(os.path.abspath(__file__))

PyInstaller.__main__.run([
    os.path.join(RAIZ, 'app', 'main.py'),
    '--name', 'cyber-sentinel',
    '--onefile',
    '--windowed',
    '--noconfirm',
    '--clean',
    '--paths', os.path.join(RAIZ, 'proto'),
    '--paths', RAIZ,
    '--hidden-import', 'system_monitor_pb2',
    '--hidden-import', 'system_monitor_pb2_grpc',
    '--collect-all', 'grpc',
    '--collect-all', 'matplotlib',
])
