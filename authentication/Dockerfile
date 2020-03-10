FROM python:3.7

WORKDIR /usr/src/app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV HOST 0.0.0.0
EXPOSE 5000
COPY . .

CMD ["python", "app"]