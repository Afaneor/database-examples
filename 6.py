from clickhouse_driver import Client
import numpy as np
from datetime import datetime, timedelta


def get_client():
    """Создание клиента ClickHouse"""
    return Client(host='localhost', port=9000)


def create_tables(client):
    """Создание таблиц для аналитики"""

    # Таблица для логов пользовательских действий
    client.execute('''
        CREATE TABLE IF NOT EXISTS user_actions (
            timestamp DateTime,
            user_id UInt32,
            action String,
            page String,
            duration_ms UInt32,
            platform String,
            country String
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, user_id)
    ''')

    # Таблица для метрик производительности
    client.execute('''
        CREATE TABLE IF NOT EXISTS performance_metrics (
            timestamp DateTime,
            service String,
            endpoint String,
            response_time_ms UInt32,
            status_code UInt16,
            error_type String DEFAULT '',
            data_size_bytes UInt32
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, service, endpoint)
    ''')


def generate_sample_data(client):
    """Генерация тестовых данных"""

    # Генерация данных о действиях пользователей
    now = datetime.now()
    user_actions = []

    actions = ['view', 'click', 'scroll', 'submit']
    pages = ['/home', '/products', '/cart', '/checkout']
    platforms = ['web', 'mobile', 'tablet']
    countries = ['US', 'UK', 'DE', 'FR', 'JP']

    for i in range(1000000):  # 1 миллион записей
        timestamp = now - timedelta(
            minutes=np.random.randint(0, 1440))  # За последние 24 часа
        user_actions.append([
            timestamp,
            np.random.randint(1, 10001),  # user_id
            np.random.choice(actions),
            np.random.choice(pages),
            np.random.randint(50, 5000),  # duration_ms
            np.random.choice(platforms),
            np.random.choice(countries)
        ])

    client.execute(
        'INSERT INTO user_actions VALUES',
        user_actions
    )

    # Генерация метрик производительности
    performance_data = []
    services = ['api', 'web', 'auth', 'payment']
    endpoints = ['/users', '/orders', '/products', '/auth']

    for i in range(500000):  # 500 тысяч записей
        timestamp = now - timedelta(minutes=np.random.randint(0, 1440))
        performance_data.append([
            timestamp,
            np.random.choice(services),
            np.random.choice(endpoints),
            np.random.randint(10, 1000),  # response_time_ms
            np.random.choice([200, 200, 200, 404, 500]),  # status_code
            '',  # error_type
            np.random.randint(100, 10000)  # data_size_bytes
        ])

    client.execute(
        'INSERT INTO performance_metrics VALUES',
        performance_data
    )


def run_analytics(client):
    """Примеры аналитических запросов"""

    # 1. Агрегация по временным интервалам
    print("\nАктивность пользователей по часам:")
    result = client.execute('''
        SELECT 
            toStartOfHour(timestamp) as hour,
            count() as actions,
            uniq(user_id) as unique_users,
            avg(duration_ms) as avg_duration
        FROM user_actions
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour
    ''')
    for row in result:
        print(
            f"Hour: {row[0]}, Actions: {row[1]}, Users: {row[2]}, Avg Duration: {row[3]:.2f}ms")

    # 2. Распределение по платформам и странам
    print("\nРаспределение пользователей по платформам и странам:")
    result = client.execute('''
        SELECT 
            platform,
            country,
            count() as actions,
            uniq(user_id) as users
        FROM user_actions
        GROUP BY platform, country
        ORDER BY users DESC
        LIMIT 10
    ''')
    for row in result:
        print(
            f"Platform: {row[0]}, Country: {row[1]}, Actions: {row[2]}, Users: {row[3]}")

    # 3. Анализ производительности сервисов
    print("\nПроизводительность сервисов:")
    result = client.execute('''
        SELECT 
            service,
            endpoint,
            count() as requests,
            avg(response_time_ms) as avg_response_time,
            quantile(0.95)(response_time_ms) as p95_response_time,
            sum(status_code = 500) as errors
        FROM performance_metrics
        GROUP BY service, endpoint
        ORDER BY avg_response_time DESC
        LIMIT 10
    ''')
    for row in result:
        print(f"Service: {row[0]}, Endpoint: {row[1]}")
        print(
            f"Requests: {row[2]}, Avg Time: {row[3]:.2f}ms, P95: {row[4]}ms, Errors: {row[5]}")

    # 4. Когортный анализ
    print("\nУдержание пользователей по дням:")
    result = client.execute('''
        WITH toDate(timestamp) as action_date
        SELECT 
            toDate(min(timestamp)) over (partition by user_id) as cohort_date,
            dateDiff('day', cohort_date, action_date) as day_number,
            count(distinct user_id) as active_users
        FROM user_actions
        GROUP BY action_date, user_id
        HAVING day_number >= 0
        ORDER BY cohort_date, day_number
        LIMIT 10
    ''')
    for row in result:
        print(f"Cohort: {row[0]}, Day: {row[1]}, Active Users: {row[2]}")


def materialized_views_example(client):
    """Пример использования материализованных представлений"""

    # Создание материализованного представления для агрегации по минутам
    client.execute('''
        CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_by_minute
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(minute)
        ORDER BY (minute, service, endpoint)
        AS SELECT
            toStartOfMinute(timestamp) as minute,
            service,
            endpoint,
            count() as requests,
            sum(response_time_ms) as total_response_time,
            sum(data_size_bytes) as total_data_size
        FROM performance_metrics
        GROUP BY minute, service, endpoint
    ''')

    # Запрос к материализованному представлению
    print("\nАгрегированные метрики по минутам:")
    result = client.execute('''
        SELECT
            minute,
            service,
            endpoint,
            requests,
            total_response_time / requests as avg_response_time
        FROM metrics_by_minute
        ORDER BY minute DESC
        LIMIT 5
    ''')
    for row in result:
        print(f"Minute: {row[0]}, Service: {row[1]}, Endpoint: {row[2]}")
        print(f"Requests: {row[3]}, Avg Response Time: {row[4]:.2f}ms")


def main():
    client = get_client()

    # Создание структуры и данных
    create_tables(client)
    generate_sample_data(client)

    # Выполнение аналитики
    run_analytics(client)
    materialized_views_example(client)


if __name__ == "__main__":
    main()