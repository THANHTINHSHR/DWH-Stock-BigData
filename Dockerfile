# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env file (optional, if you want it baked into the image,
# otherwise use env_file or environment in docker-compose.yml)
# COPY .env .

# Copy the rest of the application code (specifically the 'core' directory)
COPY ./core ./core
CMD ["python", "core/streaming/run_streaming.py"]
