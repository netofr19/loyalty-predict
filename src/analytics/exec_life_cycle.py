import pandas as pd
import sqlalchemy

def import_query(path):
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

query = import_query("life_cycle.sql")

# engines
engine_app = sqlalchemy.create_engine("sqlite:///../../data/loyalty-system/database.db")
engine_analytical = sqlalchemy.create_engine("sqlite:///../../data/analytics/database.db")

dates = [
    '2024-05-01',
    '2024-06-01',
    '2024-07-01',
    '2024-08-01',
    '2024-09-01',
    '2024-10-01',
    '2024-11-01',
    '2024-12-01',
    '2025-01-01',
    '2025-02-01',
    '2025-03-01',
    '2025-04-01',
    '2025-05-01',
    '2025-06-01',
    '2025-07-01',
    '2025-08-01',
    '2025-09-01',
    '2025-10-01',
    '2025-11-01',
    '2025-12-01',
    '2026-01-01',
    '2026-02-01',
    '2026-03-01',
    '2026-04-01',
]

for i in dates:

    print(i)

    with engine_analytical.connect() as con:
        con.execute(sqlalchemy.text(f"DELETE FROM life_cycle WHERE dtRef = date('{i}', '-1 day')"))
        con.commit()

    query_format = query.format(date=i)

    # Banco de aplicação
    df = pd.read_sql(query_format, engine_app)
    # Banco Analítico
    df.to_sql("life_cycle", engine_analytical, index=False, if_exists="append")