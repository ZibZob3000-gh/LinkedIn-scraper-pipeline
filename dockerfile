# Start from the official Ollama image (includes the runtime)
FROM ollama/ollama:latest

# Install Python (since Ollama base image is Ubuntu-based)
RUN apt-get update && apt-get install -y python3 python3-pip

# Set working directory inside the container
WORKDIR /app

# Copy your project files into the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Preload your specific model
RUN ollama pull llama3:8b-instruct-q4_K_M

# Expose Ollama’s default API port
EXPOSE 11434

# Start Ollama in the background, wait for it, then run your Python script
CMD ollama serve & sleep 5 && python main.py
