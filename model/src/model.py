import pika
import pickle
import numpy as np
import json

# Загружаем сериализованную модель из файла
with open("myfile.pkl", "rb") as file:
    model = pickle.load(file)

try:
    # Устанавливаем соединение с сервером RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq"))
    channel = connection.channel()

     # Объявляем очередь для признаков
    channel.queue_declare(queue="features")
    # Объявляем очередь для предсказанных значений
    channel.queue_declare(queue="y_pred")

    # Функция обратного вызова для обработки сообщений из очереди
    def callback(ch, method, properties, body):
        # Десериализуем полученное сообщение
        message = json.loads(body)
        message_id = message["id"]
        features = message["body"]

        print(f"Получен вектор признаков {features}")
        
        # Преобразуем признаки в массив и делаем предсказание
        prediction = model.predict(np.array(features).reshape(1, -1))

        # Формируем сообщение с результатом предсказания
        prediction_message = {"id": message_id, "body": float(prediction[0])}

        # Отправляем результат в очередь y_pred
        channel.basic_publish(
            exchange="", routing_key="y_pred", body=json.dumps(
                prediction_message)
        )

        print(f"Предсказание {prediction[0]} успешно отправлено в очередь y_pred")

    # Настроим обработчик сообщений из очереди features
    channel.basic_consume(
        queue="features", on_message_callback=callback, auto_ack=True)
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")

    # Запуск процесса обработки сообщений
    channel.start_consuming()

except Exception as e:
    # В случае ошибки при подключении к очереди выводим описание ошибки
    print(f"Не удалось подключиться к очереди: {e}")
