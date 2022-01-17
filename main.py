import requests
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import datetime as dt
import json

api_url = 'https://api-metrika.yandex.ru/stat/v1/data'
token_pre = 'OAuth '
token_path = 'token_yandex_metrica.json'   # токен Яндекс Метрики
project_id = 'My-project'   # проект в BigQuery
table_id = 'Reports.article_stat'   # датасет и таблица в BigQuery
credentials = service_account.Credentials.from_service_account_file('projeсt_bq.json')   # токен BigQuery
client = bigquery.Client(credentials=credentials, project=project_id)

# список метрик
metric_list = [
    'ym:s:visits',  # Визиты
    'ym:s:bounceRate',  # Отказы
    'ym:s:newUsers',    # Количество новых посетителей
    'ym:s:sumPublisherArticleInvolvedTimeSeconds',  # Общее время чтения, в секундах
    'ym:s:publisherviews',  # Просмотры материалов
    'ym:s:publisherusers',  # Посетители материалов
    'ym:s:publisherArticleUsersRecircled',  # Рециркуляция, посетители - количество посетителей, которые после просмотра материала перешли к другому контенту сайта
    'ym:s:publisherViewsFullScroll',    # Просмотры материалов с доскроллом - количество просмотров, в которых посетители прокрутили материал до конца
    'ym:s:publisherViewsFullRead'   # Просмотры материалов с дочтением - количество просмотров, в которых посетители прочитали материал до конца
]

# список параметров
dimension_list = [
    'ym:s:publisherArticleTitle',   # Заголовок материала
    'ym:s:publisherArticleAuthor',  # Автор
    'ym:s:publisherLongArticle',    # Длина материала
    'ym:s:publisherTrafficSource',  # Источник переходов
    'ym:s:publisherTrafficSource2', # Источник переходов (детально)
    'ym:s:publisherPageFormat'  # Формат страницы
]

# диапазон дат - выгружаем за вчера
today = dt.date.today()
date_today = str(today)
date_1 = str(today - dt.timedelta(days=1))
date_2 = str(today - dt.timedelta(days=1))
date_list = pd.date_range(start=date_1, end=date_2)


# функция для импорта статистики
def import_statistics(api_url, token_path, token_pre, date_list, date_today, metric_list, dimension_list):
    with open(token_path, 'r', encoding='utf-8') as f:
        file_content = f.read()
    token = json.loads(file_content)['token']

    # данные для авторизации
    header = {'Authorization': token_pre + token}

    statistics = pd.DataFrame()
    for date in date_list:
        print(date)
        params = {
            'ids': 160530,
            'metrics': ','.join(metric_list),
            'dimensions': ','.join(dimension_list),
            'date1': str(date).split(" ")[0],
            'date2': str(date).split(" ")[0],
            'filters': "ym:pv:URL=@'/media/'"
        }

        rest = requests.get(api_url, params=params, headers=header)
        rest_json = rest.json()

        data_import = rest_json['data']

        print('count string:', len(data_import))

        if len(data_import) > 0:
            stat_temp = pd.DataFrame()
            for i in data_import:
                dimensions = []
                for dimension in i['dimensions']:
                    dimensions.append(dimension['name'])
                metrics = i['metrics']
                df_temp = pd.DataFrame([dimensions + metrics])

                stat_temp = stat_temp.append(pd.DataFrame([dimensions + metrics]), ignore_index=True)

            dimension_names = rest_json['query']['dimensions']
            metric_names = rest_json['query']['metrics']
            stat_temp.columns = dimension_names + metric_names
            stat_temp['Date'] = rest_json['query']['date1']
            stat_temp['DateInsert'] = date_today

            statistics = statistics.append(stat_temp, ignore_index=True)
    return statistics


statistics_import = import_statistics(api_url, token_path, token_pre, date_list, date_today, metric_list,
                                      dimension_list)

# переименовываем колонки
mapping = {
    'ym:s:publisherArticleTitle': 'ArticleTitle',
    'ym:s:publisherArticleAuthor': 'ArticleAuthor',
    'ym:s:publisherLongArticle': 'LongArticle',
    'ym:s:publisherTrafficSource': 'TrafficSource',
    'ym:s:publisherTrafficSource2': 'TrafficSource2',
    'ym:s:publisherPageFormat': 'PageFormat',

    'ym:s:visits': 'Visits',
    'ym:s:bounceRate': 'BounceRate',
    'ym:s:publisherviews': 'Views',
    'ym:s:publisherusers': 'Users',
    'ym:s:newUsers': 'NewUsers',
    'ym:s:sumPublisherArticleInvolvedTimeSeconds': 'ArticleInvolvedTimeSeconds',
    'ym:s:publisherArticleUsersRecircled': 'ArticleUsersRecircled',
    'ym:s:publisherViewsFullScroll': 'ViewsFullScroll',
    'ym:s:publisherViewsFullRead': 'ViewsFullRead'
}

columns = [
    'ArticleTitle',
    'ArticleAuthor',
    'LongArticle',
    'TrafficSource',
    'TrafficSource2',
    'PageFormat',

    'Visits',
    'BounceRate',
    'NewUsers',
    'ArticleInvolvedTimeSeconds',
    'Views',
    'Users',
    'ArticleUsersRecircled',
    'ViewsFullScroll',
    'ViewsFullRead',
    'Date',
    'DateInsert'
]


def rename_columns(data, mapping, columns):
    statistics = data.rename(columns=mapping)
    statistics.columns = columns
    return statistics


statistics = rename_columns(statistics_import, mapping, columns)
statistics['Date'] = pd.to_datetime(statistics['Date'])
statistics['DateInsert'] = pd.to_datetime(statistics['DateInsert'])

# отправляем данные в BQ
job_config = ()
job = client.load_table_from_dataframe(statistics, table_id, job_config=job_config)
job.result()

print('upload from', date_1, date_2)
print('count upload string', len(statistics))
