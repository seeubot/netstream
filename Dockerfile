# Use an official Python runtime as a parent image
# Changed from buster to bullseye for active repositories
FROM python:3.10-slim-bullseye

# Install ffmpeg (which includes ffprobe) and other necessary build tools
# We use apt-get for Debian-based images
RUN apt-get update && apt-get install -y \
    ffmpeg \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed Python packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Flask will run on
EXPOSE 5000

# Run the application using Gunicorn
# Assuming your main script is named 'main.py' and your Flask app instance is 'flask_app'
# IMPORTANT: Replace 'main' with the actual name of your Python script file (without .py extension)
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "main:flask_app"]

