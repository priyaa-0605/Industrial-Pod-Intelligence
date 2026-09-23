FROM python:3.12-slim

WORKDIR /app

COPY backend/ /app/backend/
COPY frontend/ /app/frontend/

ENV PODMIND_HOST=0.0.0.0
ENV PODMIND_PORT=8765
ENV PODMIND_MODE=auto

EXPOSE 8765

CMD ["python", "backend/podmind.py"]
