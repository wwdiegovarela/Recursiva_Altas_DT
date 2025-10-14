#!/bin/bash
# Script de inicio para Cloud Run
# Cloud Run inyecta la variable PORT automáticamente

exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}

