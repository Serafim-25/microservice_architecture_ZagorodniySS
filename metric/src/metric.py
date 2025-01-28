import pika
import json
import pandas as pd

# Инициализация DataFrame для хранения полученных сообщений
messages_df = pd.DataFrame(columns=["id", "y_true", "y_pred"])

try:
    # Подключение к серверу RabbitMQ на локальной машине
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq"))
    channel = connection.channel()

    # Объявление очередей
    channel.queue_declare(queue="y_true")
    channel.queue_declare(queue="y_pred")

    # Открытие файла для записи с заголовками
    with open("./logs/metric_log.csv", "w") as log_file:
        log_file.write("id,y_true,y_pred,absolute_error\n")

    def calculate_and_log(row):
        # Рассчитывает абсолютную ошибку и записывает её в CSV
        absolute_error = abs(row["y_true"] - row["y_pred"])
        
        with open("./logs/metric_log.csv", "a") as log_file:
            log_file.write(f"{row['id']},{row['y_true']},{row['y_pred']},{absolute_error}\n")

    def callback_y_true(ch, method, properties, body):
        
        # Обработчик для очереди y_true. Обновляет DataFrame и проверяет наличие предсказания.
        
        global messages_df
        message = json.loads(body)
        message_id = message["id"]
        y_true = message["body"]
        
        # Поиск существующей строки или создание новой
        row_exists = messages_df[messages_df["id"] == message_id]
        if len(row_exists) > 0:
            # Обновление существующей строки
            messages_df.loc[row_exists.index[0], "y_true"] = y_true
        else:
            # Создание новой строки
            messages_df.loc[len(messages_df)] = {"id": message_id, "y_true": y_true, "y_pred": None}        

        # Проверка наличия предсказания для этого сообщения
        matching_pred = messages_df[
            (messages_df["id"] == message_id) & (messages_df["y_pred"].notna())
        ]

        # Проверяем, есть ли в DataFrame строка с предсказанием (y_pred) для текущего id
        if not matching_pred.empty:
             # Если строка с предсказанием найдена, получаем первую строку для этого id
            complete_row = messages_df[messages_df["id"] == message_id].iloc[0]
            # Вызываем функцию для обработки строки (вычисление абсолютной ошибки и запись в лог)
            calculate_and_log(complete_row)
            # После обработки удаляем строку с данным id из DataFrame, так как она больше не нужна
            messages_df = messages_df[messages_df["id"] != message_id]

    def callback_y_pred(ch, method, properties, body):
        
        # Обработчик для очереди y_pred. Обновляет DataFrame и проверяет наличие истинного значения.

        global messages_df
        message = json.loads(body)
        message_id = message["id"]
        y_pred = message["body"]

        # Поиск существующей строки или создание новой
        row_exists = messages_df[messages_df["id"] == message_id]
        if len(row_exists) > 0:
            # Обновление существующей строки
            messages_df.loc[row_exists.index[0], "y_pred"] = y_pred
        else:
            # Создание новой строки
            messages_df.loc[len(messages_df)] = {"id": message_id, "y_true": None, "y_pred": y_pred}

        # Проверка наличия истинного значения для этого сообщения
        matching_true = messages_df[
            (messages_df["id"] == message_id) & (messages_df["y_true"].notna())
        ]

        # Проверяем, есть ли в DataFrame строка с истинным значением (y_true) для текущего id
        if not matching_true.empty:
            # Если строка с истинным значением найдена, получаем первую строку для этого id
            complete_row = messages_df[messages_df["id"] == message_id].iloc[0]
            # Вызываем функцию для обработки строки (вычисление абсолютной ошибки и запись в лог)
            calculate_and_log(complete_row)
            # После обработки удаляем строку с данным id из DataFrame, так как она больше не нужна
            messages_df = messages_df[messages_df["id"] != message_id]

    # Устанавливаем обработчик для очереди "y_true", 
    # который будет вызывать callback_y_true при получении сообщения
    channel.basic_consume(
        queue="y_true", on_message_callback=callback_y_true, auto_ack=True
    )

    # Устанавливаем обработчик для очереди "y_pred", 
    # который будет вызывать callback_y_pred при получении сообщения
    channel.basic_consume(
        queue="y_pred", on_message_callback=callback_y_pred, auto_ack=True
    )

    # Печатаем сообщение, информируя пользователя, что приложение ожидает сообщений
    # Для выхода из процесса можно использовать сочетание клавиш CTRL+C
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    
    # Запускаем цикл обработки сообщений из очередей, блокируя выполнение до завершения работы
    channel.start_consuming()

except Exception as e:
    # В случае ошибки при подключении к очереди выводим описание ошибки
    print(f"Ошибка при подключении к очереди: {e}")
