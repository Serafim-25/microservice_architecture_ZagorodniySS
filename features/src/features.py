import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

# Бесконечный цикл для постоянной отправки данных в очереди
while True:
    try:
        # Загружаем данные о диабете: X - признаки, y - целевые значения
        X, y = load_diabetes(return_X_y=True)

        # Выбираем случайный индекс для извлечения данных
        random_row = np.random.randint(low=0, high=len(X))

        # Генерируем уникальный идентификатор для сообщения (временная метка)
        message_id = datetime.timestamp(datetime.now())

        # Устанавливаем соединение с RabbitMQ сервером
        connection = pika.BlockingConnection(
            pika.ConnectionParameters("rabbitmq"))
        channel = connection.channel()

        # Определяем две очереди: одна для целевых значений, другая для признаков
        channel.queue_declare(queue="y_true")
        channel.queue_declare(queue="features")

        # Подготавливаем сообщения: каждое включает ID и данные
        target_message = {"id": message_id, "body": float(y[random_row])}
        feature_message = {"id": message_id, "body": list(X[random_row])}

        # Отправляем сообщение с целевым значением в очередь y_true
        channel.basic_publish(
            exchange="", routing_key="y_true", body=json.dumps(target_message)
        )
        print(f"Целевое значение отправлено в очередь: {target_message}")

        # Отправляем сообщение с вектором признаков в очередь features
        channel.basic_publish(
            exchange="", routing_key="features", body=json.dumps(
                feature_message)
        )
        print(f"Признаки отправлены в очередь: {feature_message}")

        # Закрываем соединение после отправки сообщений
        connection.close()

        # Приостанавливаем выполнение программы на 8 секунд
        time.sleep(8)

    except Exception as e:
        # Логируем ошибку подключения и ждём 8 секунд перед повторной попыткой
        print(f"Не удалось подключиться к очереди: {e}")
        time.sleep(8)
