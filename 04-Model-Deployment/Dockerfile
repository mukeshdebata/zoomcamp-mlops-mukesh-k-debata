FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

# Install uv as the Python package manager
RUN pip install -U pip
RUN pip install uv

WORKDIR /app

COPY ["requirements.txt", "./"]

# Use uv to install dependencies with --system flag for global installation
RUN uv pip install --system -r requirements.txt

COPY ["src/scoring.py", "./"]

RUN mkdir -p output

CMD ["python", "scoring.py", "2023", "03"]