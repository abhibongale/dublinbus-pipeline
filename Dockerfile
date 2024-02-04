FROM jupyter/scipy-notebook:latest

# Set working directory
WORKDIR /app

# Copy all files
COPY . .

# Set working directory
RUN source /app/.env

ENV API_KEY=$API_KEY API_URL=$API_URL

# Optional: Install additional packages
RUN pip install pandas

# Expose JupyterLab port
EXPOSE 8888

# Start JupyterLab
CMD ["jupyter", "lab", "--ip", "0.0.0.0", "--allow-root"]
