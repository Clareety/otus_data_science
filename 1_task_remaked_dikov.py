import logging
import sys
import requests
import pandas as pd
import pickle
import numpy as np

owner_id = -11982368

def get_data(owner_id = -11982368):
    offset = 0
    all_posts=[]
    while True:
        r = requests.get('https://api.vk.com/method/wall.get', params = {'owner_id': owner_id,
                                                                     'access_token': ,
                                                                     'v': 5.52,
                                                                     'count':100,
                                                                     'offset': offset})
        post = r.json()#['response']
        all_posts.append(post)
        offset += 100
        if offset == 3000:
            break
    with open('getting_data_1_step.json','wb') as file:
        pickle.dump(all_posts, file)
    return all_posts

def remaster_data(all_posts = get_data()):
    df1 = pd.io.json.json_normalize(all_posts)
    cz = df1['response.items']
    data = pd.DataFrame()
    for index in range(0, len(cz)):
        a = cz[index]
        l = len(a)  # всегда 100
        dates = []

        for i in range(0, l):
            b = a[i]
            dates.append(b['date'])  # вытаскиваем даты постов
        #
        likes = []
        for i in range(0, l):
            b = a[i]
            likes.append(b['likes'])
        likes_count = []
        for i in range(0, l):
            c = likes[i]
            likes_count.append(c['count'])  # Вытаскиваем лайки
        #
        com = []
        for i in range(0, l):
            b = a[i]
            com.append(b['comments'])
        com_count = []
        for i in range(0, l):
            c = com[i]
            com_count.append(c['count'])  # вытаскиваем комментарии
        #
        rep = []
        for i in range(0, l):
            b = a[i]
            rep.append(b['reposts'])
        rep_count = []
        for i in range(0, l):
            c = rep[i]
            rep_count.append(c['count'])  # вытаскиваем репосты
            # вытаскиваем 'marked_as_ads'
        ad = []
        for i in range(0, l):
            b = a[i]
            ad.append(b['marked_as_ads'])
        text = []
        for i in range(0, l):
            b = a[i]
            text.append(b['text'])

        df = pd.DataFrame([dates, likes_count, com_count, text, rep_count, ad])
        df = df.transpose()
        data = pd.concat([data, df])
    data.rename(columns={0: 'Date', 1: 'Likes', 2: 'Comments', 3: 'Text', 4: 'Reposts', 5: 'Ad'}, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'], unit='s')
    correct_index = np.arange(3000)
    data.index = correct_index
    data.to_csv('Preparing_Data_2_Step.csv')
    return(data)

def text_from_max_populatity(data=remaster_data()):
    data['Popularity'] = data['Likes'] + (3 * data['Comments']) + (2 * data['Reposts'])
    max_pop_text = data[data['Popularity'] == data['Popularity'].max()]['Text']
    analytic_text_max = 'На самом популярном посте в Форбсе был такой - то текст {}'.format(max_pop_text)
    with open('Report.txt','a') as file_1:
        print(analytic_text_max, file = file_1)
    return(analytic_text_max)

def aggregate_week_day_popularity(data = remaster_data()):
    data['Day_of_week'] = data['Date'].apply(lambda x: x.day_name())
    data['Popularity'] = data['Likes'] + (3 * data['Comments']) + (2 * data['Reposts'])
    weekday_count = data.groupby('Day_of_week').aggregate(sum)
    result = weekday_count[['Likes', 'Popularity']]
    with open('Report.txt','a') as file_1:
        print(result, file = file_1)
    return (result)


aggregate_week_day_popularity()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logger.info("Work started")
    if sys.argv[1] == 'gather':
        get_data()
    elif sys.argv[1] == 'transform':
        remaster_data()
    elif sys.argv[1] == 'stats':
        text_from_max_populatity()
        aggregate_week_day_popularity()
logger.info("work ended")


