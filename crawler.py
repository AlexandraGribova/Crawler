import psycopg2
class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        db_name = 'Crawler'
        db_host = '127.0.0.1'
        db_user = 'postgres'
        db_password = '123456'
        db_port = '5432'
        self.conn = psycopg2.connect(dbname=db_name, host=db_host, user=db_user, password=db_password, port=db_port)
        self.conn.autocommit = True
        cursor = self.conn.cursor() ## Курсор - объект для осуществления запросов к БД
        print('Произведено подключение к серверу PostgreSQL ')
        cursor.close()
        pass

    # 0. Деструктор
    def __del__(self):
        ## Удаляем все таблицы, не факт что это нужно
        cursor = self.conn.cursor()
        sql = 'drop table linkWord;\
               drop table linkBetweenURL;\
               drop table wordLocation;\
               drop table URLList;\
               drop table wordList;'
        cursor.execute(sql)
        print('Все таблицы удалены')
        cursor.close()
        self.conn.close()
        pass

    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        pass

    # 2. Получение текста страницы
    def getTextOnly(self, text):
        return ""

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        return ""

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        return False

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=1):
        pass

    # 7. Инициализация таблиц в БД
    def initDB(self):
        cursor = self.conn.cursor()
        sql = 'create table wordList (rowId integer PRIMARY KEY,\
                                      word text,\
                                      isFiltred integer);'
        cursor.execute(sql)
        print('Создана таблица wordList')
        sql = 'create table URLList (rowId integer PRIMARY KEY,\
					                 url text);'
        cursor.execute(sql)
        print('Создана таблица URLList')
        sql = 'create table wordLocation (rowId integer PRIMARY KEY,\
                                          fk_wordId integer REFERENCES wordList(rowId),\
                                          fk_URLId integer REFERENCES URLList(rowId),\
                                          wordLocation integer);'
        cursor.execute(sql)
        print('Создана таблица wordLocation')
        sql = 'create table linkBetweenURL (rowId integer PRIMARY KEY,\
					                        fk_FromURL_Id integer REFERENCES URLList(rowId),\
						                    fk_ToURL_Id integer REFERENCES URLList(rowId));'
        cursor.execute(sql)
        print('Создана таблица linkBetweenURL')
        sql = 'create table linkWord (rowId integer PRIMARY KEY,\
                                      fk_wordId integer REFERENCES wordList(rowId),\
                                      fk_linkId integer REFERENCES linkBetweenURL(rowId));'
        cursor.execute(sql)
        print('Создана таблица linkWord')
        cursor.close()
    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value):
        return 1
