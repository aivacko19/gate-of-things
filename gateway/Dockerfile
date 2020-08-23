FROM python:3.7-alpine

WORKDIR /usr/src/app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV HOST 0.0.0.0
EXPOSE 1887
EXPOSE 8887
COPY . .

CMD ["python", "app"]