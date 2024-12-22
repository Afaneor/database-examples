from pymongo import MongoClient
from datetime import datetime
from pprint import pprint


def get_connection():
    return MongoClient('mongodb://user:password@localhost:27017/')


def crud_examples():
    # Подключение к БД
    client = get_connection()
    db = client.test_db

    # Create - Создание документов
    # Одиночная вставка
    user = {
        "email": "john@example.com",
        "name": "John Doe",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "country": "USA"
        },
        "interests": ["programming", "music", "sports"],
        "created_at": datetime.utcnow()
    }

    result = db.users.insert_one(user)
    print(f"Inserted user ID: {result.inserted_id}")

    # Массовая вставка
    orders = [
        {
            "user_id": result.inserted_id,
            "items": [
                {
                    "name": "Laptop",
                    "price": 1200,
                    "quantity": 1
                },
                {
                    "name": "Mouse",
                    "price": 25,
                    "quantity": 2
                }
            ],
            "total": 1250,
            "status": "pending",
            "shipping_address": {
                "street": "123 Main St",
                "city": "New York",
                "country": "USA"
            },
            "created_at": datetime.utcnow()
        }
    ]

    result = db.orders.insert_many(orders)
    print(f"Inserted order IDs: {result.inserted_ids}")

    # Read - Чтение документов
    # Поиск по точному совпадению
    user = db.users.find_one({"email": "john@example.com"})
    print("\nFound user:")
    pprint(user)

    # Поиск с условиями
    users = db.users.find({
        "age": {"$gte": 25},
        "interests": {"$in": ["programming", "art"]}
    })
    print("\nUsers with age >= 25 who like programming:")
    for user in users:
        pprint(user)

    # Агрегация
    pipeline = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user_orders"
            }
        },
        {
            "$project": {
                "name": 1,
                "email": 1,
                "total_orders": {"$size": "$user_orders"},
                "total_spent": {"$sum": "$user_orders.total"}
            }
        }
    ]

    results = list(db.users.aggregate(pipeline))
    print("\nUser aggregation results:")
    pprint(results)

    # Update - Обновление документов
    # Обновление одного документа
    result = db.users.update_one(
        {"email": "john@example.com"},
        {
            "$set": {"age": 31},
            "$push": {"interests": "reading"}
        }
    )
    print(f"\nModified {result.modified_count} document(s)")

    # Массовое обновление
    result = db.orders.update_many(
        {"status": "pending"},
        {"$set": {"status": "processing"}}
    )
    print(f"Modified {result.modified_count} order(s)")

    # Delete - Удаление документов
    result = db.users.delete_one({"email": "john@example.com"})
    print(f"\nDeleted {result.deleted_count} user(s)")

    # Сложный запрос с индексами
    # Создание индексов
    db.users.create_index([("email", 1)], unique=True)
    db.users.create_index([("age", 1), ("interests", 1)])

    # Поиск с использованием индексов
    users = db.users.find({
        "age": {"$gte": 25},
        "interests": "programming"
    }).hint([("age", 1), ("interests", 1)])

    client.close()


if __name__ == "__main__":
    crud_examples()