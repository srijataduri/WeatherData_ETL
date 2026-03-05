from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, date, timedelta
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def get_coordinates():
    lat = float(Variable.get("weather_latitude"))
    lon = float(Variable.get("weather_longitude"))
    tz = Variable.get("weather_timezone", default_var="America/Los_Angeles")
    return {"latitude": lat, "longitude": lon, "timezone": tz}


@task
def extract_past_60_days_weather(coords: dict):
    url = "https://archive-api.open-meteo.com/v1/archive"

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=59)

    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code",
        "timezone": coords["timezone"],
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


@task
def transform_past_60_days_weather(raw_json: dict, coords: dict):
    if "daily" not in raw_json or "time" not in raw_json["daily"]:
        raise ValueError(f"Unexpected API response keys: {list(raw_json.keys())}")

    daily = raw_json["daily"]

    times = daily["time"]
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    prec = daily.get("precipitation_sum", [])
    wcode = daily.get("weather_code", [])

    records = []
    for i in range(len(times)):
        records.append({
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "date": times[i],  # YYYY-MM-DD (Snowflake DATE accepts this)
            "temp_max": tmax[i] if i < len(tmax) else None,
            "temp_min": tmin[i] if i < len(tmin) else None,
            "precipitation": prec[i] if i < len(prec) else None,
            "weather_code": wcode[i] if i < len(wcode) else None,
        })

    return records


@task
def load(records: list):
    target_table = "RAW.WEATHER_DAILY"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL,
        date DATE NOT NULL,
        temp_max FLOAT,
        temp_min FLOAT,
        precipitation FLOAT,
        weather_code INTEGER,
        PRIMARY KEY (latitude, longitude, date)
    );
    """

    delete_sql = f"DELETE FROM {target_table};"

    insert_sql = f"""
    INSERT INTO {target_table}
    (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
    VALUES (%(latitude)s, %(longitude)s, %(date)s, %(temp_max)s, %(temp_min)s, %(precipitation)s, %(weather_code)s);
    """

    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(create_sql)
        cur.execute(delete_sql)
        cur.executemany(insert_sql, records)
        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        try:
            cur.connection.close()
        except Exception:
            pass


with DAG(
    dag_id="WeatherData_ETL",
    start_date=datetime(2026, 2, 24),
    catchup=False,
    schedule="0 * * * *",  # ✅ hourly
    tags=["ETL", "WEATHER"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    max_active_runs=1,
) as dag:

    coords = get_coordinates()
    raw = extract_past_60_days_weather(coords)
    rows = transform_past_60_days_weather(raw, coords)
    load(rows)