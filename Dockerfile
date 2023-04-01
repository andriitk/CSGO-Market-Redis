FROM --platform=linux/amd64 python:3
#FROM python:3.10

RUN mkdir /CSGO_Redis
WORKDIR /CSGO_Redis

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD python3 main.py

# docker build . -t csgo_redis:latest
# docker run -d -p 6379:6379 csgo_redis