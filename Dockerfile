FROM python:3.6.12-buster
WORKDIR /code
RUN apt-get update \
    && apt-get -y upgrade
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["flask", "crontab", "add"]
CMD ["python", "app.py"]