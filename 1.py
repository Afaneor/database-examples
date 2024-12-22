import psycopg2
from psycopg2.extras import RealDictCursor


# Подключение к БД
def get_connection():
    return psycopg2.connect(
        dbname="test_db",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )


# Создание таблиц
def create_tables(conn):
    with conn.cursor() as cur:
        # Таблица пользователей
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица заказов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                total_amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Таблица товаров
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock INTEGER NOT NULL
            )
        """)

        # Таблица товаров в заказе
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_id INTEGER REFERENCES orders(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER NOT NULL,
                price_at_time DECIMAL(10,2) NOT NULL,
                PRIMARY KEY (order_id, product_id)
            )
        """)
    conn.commit()


# Примеры CRUD операций
def crud_examples(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Create (INSERT)
        cur.execute("""
            INSERT INTO users (email, name) 
            VALUES (%s, %s) 
            RETURNING id, email, name
        """, ('john@example.com', 'John Doe'))
        user = cur.fetchone()
        print("Created user:", user)

        # Read (SELECT)
        cur.execute("SELECT * FROM users WHERE email = %s",
                    ('john@example.com',))
        user = cur.fetchone()
        print("Found user:", user)

        # Update (UPDATE)
        cur.execute("""
            UPDATE users 
            SET name = %s 
            WHERE email = %s 
            RETURNING id, email, name
        """, ('John Smith', 'john@example.com'))
        updated_user = cur.fetchone()
        print("Updated user:", updated_user)

        # Delete (DELETE)
        cur.execute("DELETE FROM users WHERE email = %s", ('john@example.com',))
        print("Deleted rows:", cur.rowcount)

    conn.commit()


# Пример сложного запроса
def complex_query_example(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                u.name,
                COUNT(o.id) as total_orders,
                SUM(o.total_amount) as total_spent,
                ARRAY_AGG(DISTINCT p.name) as purchased_products
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            LEFT JOIN order_items oi ON o.id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.id
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY total_spent DESC
            LIMIT 10
        """)
        results = cur.fetchall()
        print("Top customers:", results)


if __name__ == "__main__":
    # Пример использования
    conn = get_connection()
    try:
        create_tables(conn)
        crud_examples(conn)
        complex_query_example(conn)
    finally:
        conn.close()