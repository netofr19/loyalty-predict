import os
import pandas as pd
import sqlalchemy
from tqdm import tqdm
import datetime

BASE_DIR_QUERY = os.path.join(os.getcwd(), "src", "analytics")
BASE_DIR_DATABASE = os.path.join(os.getcwd(), "data")
os.makedirs(os.path.join(BASE_DIR_DATABASE, 'loyalty-system'), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR_DATABASE, 'analytics'), exist_ok=True)

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def date_range(start, stop):
    dates = []

    while start <= stop:
        dates.append(start)
        dt_start = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(days=1)
        start = datetime.datetime.strftime(dt_start, '%Y-%m-%d')

    return dates

query = import_query(os.path.join(BASE_DIR_QUERY, "life_cycle.sql"))

# engines
engine_app = sqlalchemy.create_engine(f"sqlite:///{os.path.join(BASE_DIR_DATABASE, 'loyalty-system', 'database.db')}")
engine_analytical = sqlalchemy.create_engine(f"sqlite:///{os.path.join(BASE_DIR_DATABASE, 'analytics', 'database.db')}")


dates = date_range('2024-09-01', '2025-10-01')

for i in (pbar := tqdm(dates)):
    pbar.set_description(f"Processing date: {i}")

    with engine_analytical.connect() as con:
        try:
            con.execute(sqlalchemy.text(f"DELETE FROM life_cycle WHERE dtRef = date('{i}', '-1 day')"))
            con.commit()
        except sqlalchemy.exc.OperationalError:
            pass

    query_format = query.format(date=i)

    # Banco de aplicação
    df = pd.read_sql(query_format, engine_app)
    # Banco Analítico
    df.to_sql("life_cycle", engine_analytical, index=False, if_exists="append")