# PAI Webhook Bridge
FROM python:3.12-alpine

# Set labels
LABEL maintainer="PAI Project"
LABEL description="PAI Webhook Bridge - MQTT to Webhook microservice"
LABEL version="1.0.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PAI_WEBHOOK_CONFIG=/app/config.yaml

# Create non-root user for security
RUN addgroup -g 1000 pai && \
    adduser -u 1000 -G pai -s /bin/sh -D pai

# Install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pai_webhook/ ./pai_webhook/

# Set ownership
RUN chown -R pai:pai /app

# Switch to non-root user
USER pai

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Default config path
VOLUME ["/app/config.yaml"]

# Entry point
CMD ["python", "-m", "pai_webhook.main"]
