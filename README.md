## Эмуляция биржевого стакана цен

Статья:
https://blog.chevich.com/emulyator-birzhevogo-stakana-czen

    docker-compose up
    python -m venv .venv .
    pip install -r requiremennts.txt

#### загрузка данных в бд
    python run_elastic.py

### запуск эмулятора
    python ws_server.py

#### .env
- DB="http://localhost:9200"

**директория где лежат файлы данные для загрузки**
- PATH_TO_FILES="/opt/binance_load_depth_ticker/"

**наименование инструмента для загрузки в бд**
- SYMBOL="atomusdt"
- WS_URL="ws://localhost:5678"
- WS_HOST="127.0.0.1"
- WS_PORT="5678"

**наименование инструмента для загрузки для ттрансляции**
- EMU_SYMBOL="atomusdt"

**дата и время начала трансляции**
- EMU_FROM="01.01.2022 10:29:20.221"