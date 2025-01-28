import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time

def plot_error_histogram():
    while True:
        try:
            # Загружаем данные из CSV-файла
            data = pd.read_csv("logs/metric_log.csv")
            data = data.dropna()  # Убираем строки с пропущенными значениями

            if len(data) > 0:
                plt.figure(figsize=(20, 15))

                # Строим гистограмму для абсолютных ошибок
                counts, bin_edges, _ = plt.hist(data["absolute_error"],
                                            bins=10,  # Количество интервалов
                                            range=(0, 150),  # Задаём диапазон для значений
                                            color='lime',  # Задаём зелёный цвет для столбцов
                                            edgecolor='black')  # Чёрные границы столбцов

                # Считаем центры столбцов для сглаженной линии
                bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                plt.plot(bin_centers, counts, '-', color='green', linewidth=2)  # Добавляем линию через средние точки

                # Настройка подписей осей
                plt.xlabel("absolute_error")
                plt.ylabel("count")

                # Убираем верхнюю и правую границы графика
                plt.gca().spines['top'].set_visible(False)
                plt.gca().spines['right'].set_visible(False)

                # Ограничиваем пределы осей
                plt.ylim(0, 40)
                plt.xlim(0, 150)

                # Сохраняем изображение в файл
                plt.savefig("logs/error_distribution.png")
                plt.close()

            time.sleep(10)  # Задержка перед следующей итерацией

        except Exception as e:
            print(f"Произошла ошибка: {e}")
            time.sleep(10)  # Если ошибка, ждём 10 секунд и пробуем снова

if __name__ == "__main__":
    plot_error_histogram()  # Запуск функции