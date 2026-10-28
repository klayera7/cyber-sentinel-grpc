#!/bin/sh
set -e

# Sobe o Ollama (embutido na imagem) em background e aguarda a API responder
ollama serve &

until ollama list >/dev/null 2>&1; do
    sleep 1
done

# python vira o PID 1; ao sair, o container para e o ollama morre junto
exec python3 /app/server/main.py
