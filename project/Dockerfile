FROM python:3.10-slim

WORKDIR /vnpy_ddb

COPY . /vnpy_ddb

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

RUN tar -xvzf ta-lib-0.4.0-src.tar.gz -C /tmp \
    && cd /tmp/ta-lib \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && rm -rf /tmp/ta-lib

RUN pip install --no-cache-dir -r requirements.txt

ENV SYMBOL_FILE_PATH=/vnpy_ddb/symbol.json

ENV ACCOUNT__FILE_PATH=/vnpy_ddb/account.json

ENV HISTORY_FILE_PASH=/vnpy_ddb/history.csv

CMD ["python", "demo_live.py"]