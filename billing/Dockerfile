# Image for the billing subsystem services (control plane, gateway, init jobs).
FROM python:3.11-slim

WORKDIR /app
COPY billing/requirements.txt /app/billing/requirements.txt
RUN pip install --no-cache-dir -r /app/billing/requirements.txt

COPY billing /app/billing

# Default to the control plane; compose overrides `command` per service.
EXPOSE 8100 8080
CMD ["uvicorn", "billing.main:control_plane_app", "--host", "0.0.0.0", "--port", "8100"]
