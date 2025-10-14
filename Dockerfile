# Use the official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .
COPY start.sh .

# Make the start script executable
RUN chmod +x start.sh

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run the application
CMD ["./start.sh"]
