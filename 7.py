import redis
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any


class RedisExample:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis = redis.Redis(host=host, port=port, db=db,
                                 decode_responses=True)

    def basic_operations(self):
        """Базовые операции с Redis"""
        print("\n=== Базовые операции ===")

        # Строки (Strings)
        self.redis.set('user:1:name', 'John Doe')
        self.redis.set('user:1:email', 'john@example.com')
        self.redis.set('user:1:visits', 1,
                       nx=True)  # Установить, только если не существует

        # Инкремент
        self.redis.incr('user:1:visits')
        visits = self.redis.get('user:1:visits')
        print(f"Количество визитов: {visits}")

        # Установка с истечением
        self.redis.setex('temporary_key', 60, 'will expire in 60 seconds')
        ttl = self.redis.ttl('temporary_key')
        print(f"Оставшееся время жизни: {ttl} секунд")

    def caching_example(self):
        """Пример использования Redis для кэширования"""
        print("\n=== Кэширование ===")

        def get_expensive_data(user_id: int) -> dict:
            """Имитация получения данных из медленной БД"""
            time.sleep(1)  # Имитация долгой работы
            return {
                'id': user_id,
                'name': 'John Doe',
                'email': 'john@example.com',
                'preferences': {'theme': 'dark', 'language': 'en'}
            }

        def get_user_data(user_id: int) -> Dict[str, Any]:
            """Получение данных с использованием кэша"""
            cache_key = f'user:{user_id}:data'

            # Пробуем получить из кэша
            cached_data = self.redis.get(cache_key)
            if cached_data:
                print("Данные получены из кэша")
                return json.loads(cached_data)

            # Если нет в кэше, получаем из БД
            print("Получение данных из БД...")
            data = get_expensive_data(user_id)

            # Сохраняем в кэш на 5 минут
            self.redis.setex(cache_key, 300, json.dumps(data))
            return data

        # Пример использования
        start = time.time()
        data = get_user_data(1)  # Первый запрос (медленный)
        print(f"Первый запрос: {time.time() - start:.2f} сек")

        start = time.time()
        data = get_user_data(1)  # Второй запрос (быстрый, из кэша)
        print(f"Второй запрос: {time.time() - start:.2f} сек")

    def session_management(self):
        """Пример управления сессиями"""
        print("\n=== Управление сессиями ===")

        def create_session(user_id: int) -> str:
            """Создание сессии"""
            session_id = f"sess_{int(time.time())}"
            session_data = {
                'user_id': user_id,
                'login_time': datetime.now().isoformat(),
                'last_activity': datetime.now().isoformat()
            }

            # Сохраняем сессию на 30 минут
            self.redis.hset(f'session:{session_id}', mapping=session_data)
            self.redis.expire(f'session:{session_id}', 1800)

            return session_id

        def get_session(session_id: str) -> Optional[Dict]:
            """Получение данных сессии"""
            session_data = self.redis.hgetall(f'session:{session_id}')
            if session_data:
                # Обновляем время последней активности
                self.redis.hset(f'session:{session_id}', 'last_activity',
                                datetime.now().isoformat())
                # Продлеваем время жизни сессии
                self.redis.expire(f'session:{session_id}', 1800)
            return session_data if session_data else None

        # Пример использования
        session_id = create_session(user_id=1)
        print(f"Создана сессия: {session_id}")

        session = get_session(session_id)
        print(f"Данные сессии: {session}")

    def rate_limiting(self):
        """Пример ограничения частоты запросов"""
        print("\n=== Ограничение частоты запросов ===")

        def check_rate_limit(user_id: int, limit: int = 5,
                             window: int = 60) -> bool:
            """
            Проверка ограничения частоты запросов
            limit: максимальное количество запросов
            window: временное окно в секундах
            """
            key = f'ratelimit:{user_id}'
            current = self.redis.get(key)

            if not current:
                # Первый запрос
                self.redis.setex(key, window, 1)
                return True

            current = int(current)
            if current >= limit:
                return False

            # Увеличиваем счетчик
            self.redis.incr(key)
            return True

        # Пример использования
        user_id = 1
        for i in range(7):
            allowed = check_rate_limit(user_id)
            print(f"Запрос {i + 1}: {'разрешен' if allowed else 'отклонен'}")

    def pub_sub_example(self):
        """Пример публикации и подписки"""
        print("\n=== Публикация и подписка ===")

        def publish_event(channel: str, message: dict):
            """Публикация события"""
            self.redis.publish(channel, json.dumps(message))

        # Пример публикации событий
        publish_event('notifications', {
            'type': 'user_registered',
            'user_id': 1,
            'timestamp': datetime.now().isoformat()
        })

        print("Событие опубликовано")

    def sorted_set_example(self):
        """Пример работы с сортированными множествами"""
        print("\n=== Сортированные множества ===")

        # Добавляем очки игроков
        self.redis.zadd('leaderboard', {
            'player1': 100,
            'player2': 200,
            'player3': 150,
            'player4': 300
        })

        # Получаем топ-3 игроков
        top_players = self.redis.zrevrange('leaderboard', 0, 2, withscores=True)
        print("Топ 3 игрока:")
        for player, score in top_players:
            print(f"{player}: {int(score)}")

        # Получаем ранг игрока
        rank = self.redis.zrevrank('leaderboard', 'player1')
        print(f"Ранг player1: {rank + 1}")


def main():
    redis_example = RedisExample()

    # Очистка базы данных перед демонстрацией
    redis_example.redis.flushdb()

    # Запуск примеров
    redis_example.basic_operations()
    redis_example.caching_example()
    redis_example.session_management()
    redis_example.rate_limiting()
    redis_example.pub_sub_example()
    redis_example.sorted_set_example()


if __name__ == "__main__":
    main()