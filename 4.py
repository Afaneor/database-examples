import chromadb
from chromadb.config import Settings
import numpy as np


def get_client():
    """Создание клиента ChromaDB"""
    return chromadb.Client(Settings(
        chroma_api_impl="rest",
        host="localhost",
        port="8000"
    ))


def text_search_example():
    """Пример поиска похожих текстов"""
    client = get_client()

    # Создаем коллекцию
    collection = client.create_collection(
        name="Нормативные документы",
    )

    # Добавляем документы
    documents = [
        "Python is a popular programming language",
        "JavaScript is used for web development",
        "Machine learning is a subset of artificial intelligence",
        "Deep learning uses neural networks",
        "Python is great for AI development"
    ]

    collection.add(
        documents=documents,
        ids=[f"doc_{i}" for i in range(len(documents))]
    )

    # Поиск похожих документов
    results = collection.query(
        query_texts=["programming with Python"],
        n_results=2
    )

    print("\nПоиск похожих текстов:")
    for i, doc in enumerate(results['documents'][0]):
        print(f"Match {i + 1}: {doc}")
        print(f"Distance: {results['distances'][0][i]}")


def image_search_example():
    """Пример поиска похожих изображений"""
    client = get_client()

    # Создаем коллекцию для векторов изображений
    collection = client.create_collection(
        name="image_vectors"
    )

    # Симулируем векторы изображений (обычно получаются из CNN)
    def generate_image_vector():
        return np.random.rand(512).tolist()  # 512-мерный вектор

    # Добавляем векторы изображений
    image_vectors = [generate_image_vector() for _ in range(5)]
    image_metadata = [
        {"filename": f"image_{i}.jpg", "category": "cat" if i < 3 else "dog"}
        for i in range(5)
    ]

    collection.add(
        embeddings=image_vectors,
        ids=[f"img_{i}" for i in range(5)],
        metadatas=image_metadata
    )

    # Поиск похожих изображений
    query_vector = generate_image_vector()
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=2
    )

    print("\nПоиск похожих изображений:")
    for i, img_id in enumerate(results['ids'][0]):
        print(f"Match {i + 1}: {img_id}")
        print(f"Metadata: {results['metadatas'][0][i]}")
        print(f"Distance: {results['distances'][0][i]}")


def semantic_search_example():
    """Пример семантического поиска с фильтрацией"""
    client = get_client()

    collection = client.create_collection(
        name="products",
        metadata={"description": "Product catalog"}
    )

    # Добавляем продукты с эмбеддингами и метаданными
    products = [
        {
            "text": "iPhone 13 Pro, high-end smartphone with great camera",
            "metadata": {"category": "electronics", "price": 999,
                         "brand": "Apple"}
        },
        {
            "text": "Samsung Galaxy S21, powerful Android phone",
            "metadata": {"category": "electronics", "price": 799,
                         "brand": "Samsung"}
        },
        {
            "text": "MacBook Pro 16, professional laptop for developers",
            "metadata": {"category": "computers", "price": 2399,
                         "brand": "Apple"}
        }
    ]

    # Симулируем эмбеддинги (обычно генерируются моделью)
    embeddings = [np.random.rand(512).tolist() for _ in range(len(products))]

    collection.add(
        embeddings=embeddings,
        documents=[p["text"] for p in products],
        metadatas=[p["metadata"] for p in products],
        ids=[f"prod_{i}" for i in range(len(products))]
    )

    # Поиск с фильтрацией
    results = collection.query(
        query_embeddings=[np.random.rand(512).tolist()],
        n_results=2,
        where={"category": "electronics", "price": {"$lt": 1000}}
    )

    print("\nСемантический поиск с фильтрацией:")
    for i, doc in enumerate(results['documents'][0]):
        print(f"Match {i + 1}: {doc}")
        print(f"Metadata: {results['metadatas'][0][i]}")
        print(f"Distance: {results['distances'][0][i]}")


if __name__ == "__main__":
    text_search_example()
    image_search_example()
    semantic_search_example()