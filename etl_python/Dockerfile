FROM python:3.9

# Set work directory
WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --nocache-dir -r requirements.txt

# Copy directories
COPY ./data ./data
COPY ./src ./src
WORKDIR /app/src

CMD["python3", "main.py"]
