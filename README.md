# Api-yandex-metrica-content
Небольшая программа для импорта контентной статистики из Яндекс Метрики и загрузки данных в Google BigQuery.

В Яндекс Метрике есть отчеты, посвященные контенту. Это могут быть статьи, блоги и прочее. Для работы написал небольшую программу, которая по API Яндекс Метрики ежеденевно получает стастистику по контенту и загружает ее в Google BigQuery/ Дальше ее использую в своих отчетах.

Для того, чтобы успешно выгрузить статистику, сначала нужно получить токен приложения для получения статистики из Яндекс Метрики. Сделать это можно здесь: https://oauth.yandex.ru/client/new.

Документация API Яндекс Метрики находится здесь: https://yandex.ru/dev/metrika/doc/api2/concept/about.html.

Параметры для контентных отчетов находятся здесь: https://yandex.ru/dev/metrika/doc/api2/api_v1/attributes/visits/publisher.html.

Метрики для контентных отчетов находятся здесь: https://yandex.ru/dev/metrika/doc/api2/api_v1/metrics/visits/publisher.html.
