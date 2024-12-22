from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record for record in result]


def create_social_network_example(connection):
    """Пример создания социальной сети"""

    # Создаем пользователей
    create_users = """
    CREATE (alice:Person {name: 'Alice', age: 30, joined: datetime()})
    CREATE (bob:Person {name: 'Bob', age: 35, joined: datetime()})
    CREATE (charlie:Person {name: 'Charlie', age: 25, joined: datetime()})
    CREATE (david:Person {name: 'David', age: 28, joined: datetime()})

    // Создаем отношения дружбы
    CREATE (alice)-[:FRIENDS {since: datetime()}]->(bob)
    CREATE (bob)-[:FRIENDS {since: datetime()}]->(charlie)
    CREATE (charlie)-[:FRIENDS {since: datetime()}]->(david)
    CREATE (alice)-[:FRIENDS {since: datetime()}]->(david)

    // Создаем посты
    CREATE (post1:Post {content: 'Hello Neo4j!', created: datetime()})
    CREATE (post2:Post {content: 'Graph databases are awesome', created: datetime()})

    // Создаем отношения к постам
    CREATE (alice)-[:POSTED]->(post1)
    CREATE (bob)-[:POSTED]->(post2)
    CREATE (charlie)-[:LIKED {timestamp: datetime()}]->(post1)
    CREATE (david)-[:LIKED {timestamp: datetime()}]->(post1)
    """

    connection.run_query(create_users)
    print("Социальная сеть создана")


def relationship_queries(connection):
    """Примеры запросов по отношениям"""

    # 1. Найти друзей друзей
    friends_of_friends = """
    MATCH (person:Person {name: 'Alice'})-[:FRIENDS]->(friend)-[:FRIENDS]->(friend_of_friend)
    WHERE friend_of_friend <> person
    RETURN DISTINCT friend_of_friend.name as name
    """

    result = connection.run_query(friends_of_friends)
    print("\nДрузья друзей Alice:")
    for record in result:
        print(record['name'])

    # 2. Найти самый популярный пост (по лайкам)
    popular_posts = """
    MATCH (post:Post)<-[like:LIKED]-()
    WITH post, COUNT(like) as likes
    RETURN post.content as content, likes
    ORDER BY likes DESC
    """

    result = connection.run_query(popular_posts)
    print("\nПопулярные посты:")
    for record in result:
        print(f"Content: {record['content']}, Likes: {record['likes']}")

    # 3. Найти путь между пользователями
    path_between_users = """
    MATCH path = shortestPath((start:Person {name: 'Alice'})-[:FRIENDS*]-(end:Person {name: 'Charlie'}))
    RETURN [node IN nodes(path) | node.name] as path
    """

    result = connection.run_query(path_between_users)
    print("\nКратчайший путь между Alice и Charlie:")
    for record in result:
        print(" -> ".join(record['path']))


def recommendations_example(connection):
    """Пример рекомендательной системы"""

    # Рекомендации друзей на основе общих связей
    friend_recommendations = """
    MATCH (person:Person {name: 'Alice'})-[:FRIENDS]->(friend)-[:FRIENDS]->(potential_friend)
    WHERE NOT (person)-[:FRIENDS]->(potential_friend)
    AND person <> potential_friend
    WITH potential_friend, COUNT(friend) as common_friends
    RETURN potential_friend.name as recommended_friend, common_friends
    ORDER BY common_friends DESC
    """

    result = connection.run_query(friend_recommendations)
    print("\nРекомендации друзей для Alice:")
    for record in result:
        print(f"Рекомендуется: {record['recommended_friend']}, "
              f"Общих друзей: {record['common_friends']}")

    # Рекомендации постов на основе лайков друзей
    post_recommendations = """
    MATCH (person:Person {name: 'Alice'})-[:FRIENDS]->(friend)-[:LIKED]->(post:Post)
    WHERE NOT (person)-[:LIKED]->(post)
    AND NOT (person)-[:POSTED]->(post)
    WITH post, COUNT(friend) as friend_likes
    RETURN post.content as content, friend_likes
    ORDER BY friend_likes DESC
    """

    result = connection.run_query(post_recommendations)
    print("\nРекомендации постов для Alice:")
    for record in result:
        print(f"Пост: {record['content']}, "
              f"Лайков от друзей: {record['friend_likes']}")


def graph_algorithms_example(connection):
    """Пример использования графовых алгоритмов"""

    # Подсчет PageRank для пользователей
    pagerank_query = """
    CALL gds.graph.project(
        'socialNetwork',
        'Person',
        'FRIENDS'
    )
    YIELD graphName

    CALL gds.pageRank.write('socialNetwork', {
        writeProperty: 'pageRank'
    })
    YIELD nodePropertiesWritten

    MATCH (p:Person)
    RETURN p.name as name, p.pageRank as rank
    ORDER BY rank DESC
    """

    try:
        result = connection.run_query(pagerank_query)
        print("\nPageRank пользователей:")
        for record in result:
            print(f"User: {record['name']}, Rank: {record['rank']:.4f}")
    except Exception as e:
        print("Для работы с алгоритмами требуется плагин Graph Data Science")


def main():
    # Подключение к Neo4j
    connection = Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="test1234"
    )

    try:
        # Очистка базы данных
        connection.run_query("MATCH (n) DETACH DELETE n")

        # Запуск примеров
        create_social_network_example(connection)
        relationship_queries(connection)
        recommendations_example(connection)
        graph_algorithms_example(connection)

    finally:
        connection.close()


if __name__ == "__main__":
    main()