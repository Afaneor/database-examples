from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import numpy as np


def get_client():
    """Создание клиента InfluxDB"""
    return InfluxDBClient(
        url="http://localhost:8086",
        token="your-token",
        org="myorg"
    )


def write_sensor_data(write_api, bucket):
    """Пример записи данных с сенсоров"""
    # Симуляция данных с температурных сенсоров
    current_time = datetime.utcnow()

    # Создаем данные для нескольких сенсоров
    for sensor_id in range(1, 4):
        for minutes_ago in range(60):
            # Симуляция температуры с небольшим шумом
            temperature = 20 + np.sin(minutes_ago / 10) + np.random.normal(0,
                                                                           0.5)
            humidity = 50 + np.cos(minutes_ago / 10) + np.random.normal(0, 2)

            point = Point("sensor_readings") \
                .tag("sensor_id", f"sensor_{sensor_id}") \
                .tag("location", f"room_{sensor_id}") \
                .field("temperature", float(temperature)) \
                .field("humidity", float(humidity)) \
                .time(current_time - timedelta(minutes=minutes_ago))

            write_api.write(bucket=bucket, record=point)


def query_examples(query_api, bucket):
    """Примеры различных запросов"""

    # 1. Простой запрос - средняя температура за последний час
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
        |> filter(fn: (r) => r["_field"] == "temperature")
        |> mean()
    '''
    result = query_api.query(query)
    print("\nСредняя температура за последний час:")
    for table in result:
        for record in table.records:
            print(f"Mean temperature: {record.get_value():.2f}°C")

    # 2. Агрегация по временным окнам
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
        |> filter(fn: (r) => r["_field"] == "temperature")
        |> window(every: 10m)
        |> mean()
        |> duplicate(column: "_stop", as: "time")
        |> window(every: inf)
    '''
    result = query_api.query(query)
    print("\nСредняя температура по 10-минутным интервалам:")
    for table in result:
        for record in table.records:
            print(
                f"Time: {record.get_time()}, Temperature: {record.get_value():.2f}°C")

    # 3. Поиск аномалий (значения выше определенного порога)
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
        |> filter(fn: (r) => r["_field"] == "temperature")
        |> filter(fn: (r) => r["_value"] > 22)
    '''
    result = query_api.query(query)
    print("\nАномальные значения температуры (>22°C):")
    for table in result:
        for record in table.records:
            print(
                f"Time: {record.get_time()}, Sensor: {record.values.get('sensor_id')}, "
                f"Temperature: {record.get_value():.2f}°C")

    # 4. Расчет производных метрик
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r["_measurement"] == "sensor_readings")
        |> filter(fn: (r) => r["_field"] == "temperature")
        |> derivative(unit: 1m)
    '''
    result = query_api.query(query)
    print("\nСкорость изменения температуры (°C/мин):")
    for table in result:
        for record in table.records:
            print(
                f"Time: {record.get_time()}, Change Rate: {record.get_value():.3f}°C/min")


def main():
    client = get_client()
    bucket = "mybucket"

    try:
        # Запись данных
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_sensor_data(write_api, bucket)
        print("Data written successfully")

        # Выполнение запросов
        query_api = client.query_api()
        query_examples(query_api, bucket)

    finally:
        client.close()


if __name__ == "__main__":
    main()