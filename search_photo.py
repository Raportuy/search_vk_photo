import datetime
import decimal
from playsound import playsound
import pandas as pd
import vk_api
from tqdm import tqdm

# функция разбивает диапазон дат на несколько дат
def range_date(start, end, step):
    while end != start:
        yield [
            datetime.datetime.timestamp(start),
            datetime.datetime.timestamp(start + step),
        ]
        start += step

# функция разбивает списки на несколько списков размерностью n
def batch(iterable: list, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


"""
Задаём переменные
"""
# токен от вконтакте
TOKEN = ""
if TOKEN == "":
    raise SystemExit("В переменной TOKEN отсутствует токен..")
# путь входного файла
path_in = "photo-sample.csv"
# путь выходного файла
path_out = "photo-output"
# пропускать группы? True - да
group = False

vk_session_user = vk_api.VkApi(token=TOKEN, api_version=5.103)
# клиент для юзера
vk_client_user = vk_session_user.get_api()

massive_photos = []
split = input("Разделить файл на несколько файлов? True/False: ")
if split:
    length = int(input("Сколько строк на один файл? Например 10000: "))
date_start = input("Введите НАЧАЛЬНЫЙ диапазон поиска в формате дд.мм.гггг: ")
date_start = datetime.datetime.strptime(date_start, "%d.%m.%Y")
date_end = input("Введите КОНЕЧНЫЙ диапазон поиска в формате дд.мм.гггг: ")
date_end = datetime.datetime.strptime(date_end, "%d.%m.%Y")
date_list = list(range_date(date_start, date_end, datetime.timedelta(days=1)))

longitude = input("Введите долготу: ")
latitude = input("Введите широту: ")
radius = input("Введите радиус поиска, например 10 или 50000, по-умолчанию 5000: ")
if radius is None:
    radius = 5000


def func():
    print("Старт\n")
    for dt in tqdm(date_list):
        offset = 0
        while True:
            photos = vk_client_user.photos.search(
                lat=latitude,
                long=longitude,
                start_time=dt[0],
                end_time=dt[1],
                offset=offset,
                count=1000,
                radius=radius,
            )
            if photos["items"] == []:
                break
            for photo in photos["items"]:
                if group:
                    # если группа - пропускаем, увеличиваем счётчик
                    if photo["owner_id"] < 0:
                        continue
                # если фото не содержит координат
                if not photo.get("long"):
                    # пропускаем
                    continue
                massive_photos.append(
                    [
                        f"https://vk.com/id{photo['owner_id']}",
                        f"https://vk.com/photo{photo['owner_id']}_{photo['id']}",
                        photo["sizes"][-1]["url"],
                        datetime.datetime.fromtimestamp(photo["date"]).strftime(
                            "%d.%m.%Y %H:%M"
                        ),
                        photo["long"],
                        photo["lat"],
                    ]
                )
            offset += 1000
    return massive_photos


massive_photos = func()
# если разделять
if split:
    count = 0
    for files_batch in batch(massive_photos, length):
        my_list = [["url_profile", "address_photo", "url", "date", "longitude", "latitude"]]
        fieldnames = files_batch[0]
        for values in files_batch[1:]:
            inner_dict = dict(zip(fieldnames, values))
            my_list.append(inner_dict)
        df = pd.DataFrame(data=my_list)
        file_name = f"{path_out}-{count}.csv"
        df.to_csv(file_name, index=False)
        print(f"Файл {file_name} сохранён.")
        count += 1
else:
    my_list = []
    fieldnames = massive_photos[0]
    for values in massive_photos[1:]:
        inner_dict = dict(zip(fieldnames, values))
        my_list.append(inner_dict)
    df = pd.DataFrame(data=my_list)
    file_name = f"{path_out}.csv"
    df.to_csv(file_name, index=False)
    print(f"Файл {file_name} сохранён.")

# воспроизводим звук завершения поиска фото
playsound("sound.mp3")
