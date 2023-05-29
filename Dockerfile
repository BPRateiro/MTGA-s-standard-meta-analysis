FROM python:3.11

WORKDIR /untapped

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ./src ./src

CMD ["python", "./src/main.py"]