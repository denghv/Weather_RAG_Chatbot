FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY chatbot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY chatbot/app.py .
COPY chatbot/weather_alerts.py .
COPY chatbot/forecast_handler.py .
COPY chatbot/templates/ ./templates/
COPY chatbot/location_mapping.py .

# Copy provinces.py file
COPY provinces.py .

# Expose port for the Flask app
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
