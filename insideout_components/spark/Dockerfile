# Use the Bitnami Spark image as the base
FROM docker.io/bitnami/spark:3.4.3

# Switch to root to install Python packages
USER root

# Install necessary Python libraries
RUN pip install --upgrade pip
RUN pip install beautifulsoup4==4.12.3 pandas==2.2.2

# Switch back to the spark user
USER 1001
