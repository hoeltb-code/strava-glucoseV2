#!/bin/bash

# Force unbuffered stdout/stderr so platform log timestamps stay close to event time.
export PYTHONUNBUFFERED=1

exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
