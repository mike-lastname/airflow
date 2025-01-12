
### Airflow

#### DAG file_generator_dag

Генерирует случайные файлы и загружает на Я.Диск, изменяет существующие файлы.  
DAG запускается 1 раз в час.

1. Генерация новых файлов
   - Генерирует случайное количество файлов (от 1 до 50) со случайным содержимым  
   (строка длиной 100 символов).
   - Создает в определенной папке (путь указывается в переменных Airflow)  
   на Я.Диске папку с меткой даты и времени.
   - Загружает сгенерированные файлы в созданную папку.
2. Изменение существующих файлов
   - выбираются случайные 5 файлов и в конце добавляется строка "random string".

#### DAG sync_dag

Загружает файлы из папки Я.Диска в локальную папку.  
DAG запускается 1 раз в 5 минут.  

1. Выбираются файлы с временем модификации больше, чем время предыдущего запуска DAG.
2. В DAG есть опция hard_sync. При её включении удаляются все файлы в локальной папке  
и заново скачиваются все файлы из папки Я.Диска.  
Для загрузки и скачивания файлов с Я.Диск используется модуль yadisk.  
Token для доступа к Я.Диск хранится в переменных Airflow.

#### Функции utils.py

`random_string_generator` - генератор строки из случайных символов  
(прописные и строчные латинские буквы, цифры).  
`generator` - генератор случайных файлов.  
`uploader` - загружает сгенерированные файлы на Я.Диск.  
`random_edit` - изменение случайных файлов на Я.Диске.  
В конец изменяемых файлов добавляется строка "random string".  
`get_files_to_dl` - возвращает список файлов для загрузки в локальную папку.  
Сравнивается время изменения файла и время предыдущего запуска sync_dag.  
`delete_all` - удаляет все файлы в локальной папке.  
`download_files` - загружает файлы с Я.Диска в локальную папку.  
`size_converter` - конвертер из байтов в кбайты, мбайты, гбайты.  
`disk_space_info` - считает занятое и свободное место на Я.Диске.  

### Pyspark

#### Топ 5 высокооцененных жанров фильмов за 3 последних десятилетия

Используются данные IMDB:

Таблица **title.basics**  
tconst (string) - alphanumeric unique identifier of the title \
titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc) \
primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release \
originalTitle (string) - original title, in the original language \
isAdult (boolean) - 0: non-adult title; 1: adult title \
startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year \
endYear (YYYY) – TV Series end year. '\N' for all other title types \
runtimeMinutes – primary runtime of the title, in minutes \
genres (string array) – includes up to three genres associated with the title \
https://datasets.imdbws.com/title.basics.tsv.gz

Таблица **title.ratings**  
tconst (string) - alphanumeric unique identifier of the title \
averageRating – weighted average of all the individual user ratings \
numVotes - number of votes the title has received \
https://datasets.imdbws.com/title.ratings.tsv.gz

Данные загружаются в локальную БД (используется PostgreSQL), затем в датафрейм Pyspark.  
`pyspark_imdb_top5_window.ipynb` - расчет топ 5 жанров

#### Выявление корреляции между длиной названия и фильма и его оценкой

Используются данные IMDB **title.basics** и **title.ratings**  
`pyspark_imdb_corr.ipynb` - расчет корреляции 