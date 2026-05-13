FROM europe-west3-docker.pkg.dev/serca-artifact-registry/virtual-docker/python:3.8-slim

WORKDIR /app
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD  python main.py
