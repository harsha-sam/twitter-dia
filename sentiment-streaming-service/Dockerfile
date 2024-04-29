FROM python:3

WORKDIR /usr/src/app
RUN apt-get update && \
  apt-get install -y openjdk-17-jdk && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*


COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "app.py"]