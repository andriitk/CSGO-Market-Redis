FROM --platform=linux/amd64 python:3
#FROM python:3.10

RUN mkdir /CSGO_Redis

WORKDIR /CSGO_Redis

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN alembic upgrade herad

CMD python3 main.py