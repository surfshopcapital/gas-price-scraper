FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3.10-venv \
    python3-pip \
    wget \
    gnupg \
    ca-certificates \
    procps \
    libxss1 \
    && rm -rf /var/lib/apt/lists/*

# Create symlinks for python and pip (remove existing first)
RUN rm -f /usr/bin/python /usr/bin/pip
RUN ln -s /usr/bin/python3.10 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright with ALL system dependencies
RUN playwright install --with-deps chromium

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV NONINTERACTIVE=1

# Expose port (if needed)
EXPOSE 8000

# Start the application
CMD ["python", "run_scraper.py"]
