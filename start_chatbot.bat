@echo off
REM Set the OpenAI API key as an environment variable
set OPENAI_API_KEY=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

REM Start the Docker containers
docker-compose up -d

echo Chatbot started! Access it at http://localhost:5000
