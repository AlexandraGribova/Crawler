import psycopg2
import bs4
import re
import requests
class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        db_name = 'Crawler'
        db_host = '127.0.0.1'
        db_user = 'postgres'
        db_password = '123456'
        db_port = '5432'
        self.start_link = 'https://ngs.ru/'
        self.conn = psycopg2.connect(dbname=db_name, host=db_host, user=db_user, password=db_password, port=db_port)
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        print('Произведено подключение к серверу')
        sql = 'select \'create database %s\' where NOT EXISTS (SELECT FROM pg_database WHERE datname = \'%s\')' % (db_name, db_name)
        cursor.execute(sql) ## Если БД уже существует, то ничего не вернет, иначе вернет 'ctreate db name'
        res = cursor.fetchall()
        if len(res) != 0:
            cursor.execute(res)
        print('Создана БД Crawler')
        ## Удаляем все таблицы\
        sql = 'drop table linkWord;\
                       drop table linkBetweenURL;\
                       drop table wordLocation;\
                       drop table URLList;\
                       drop table wordList;'
        cursor.execute(sql)
        print('Все таблицы удалены')
        cursor.close()
        pass

    # 0. Деструктор
    def __del__(self):

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

    # Добавление ссылки в URLList с проверкой на наличие дублей
    def addUrlToURLList(self, url):
        cursor = self.conn.cursor()
        cursor.execute("""select * from URLList where url=%s""", [url])
        result = cursor.fetchall()
        #   добавить в таблицу URLList если такой записи нет
        if len(result) == 0:
           cursor.execute("""insert  into URLList(url) values(%s);""", [url])
        pass
    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth=1):
        cursor = self.conn.cursor()
        urlListNew = urlList.copy()
        #   добавить в таблицу URLList стартовую ссылку (на нгс)
        cursor.execute("""insert into URLList(url) values(%s);""", [self.start_link])
        for currDepth in range(0, maxDepth):
            urlList = urlListNew.copy()
            urlListNew = []
            for url in urlList:
                # получить HTML-код страницы по текущему url
                html_doc = requests.get(url).text
                # использовать парсер для работа тегов
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                # получить список тэгов <a> с текущей страницы
                for a_tag in soup.findAll('a'):
                    #   проверить наличие атрибута 'href'
                    href_tag = a_tag.get('href')
                    #   убрать пустые ссылки, вырезать якоря из ссылок, и т.д.
                    if href_tag is not None and not '#' in href_tag and href_tag!='/':#!! вот тут непонятные локальые ссылки не удаляю пока что
                        if not 'http' in href_tag:
                            href_tag = url[:-1] + href_tag
                        #   добавить ссылку в список следующих на обход
                        urlListNew.append(href_tag)
                        #   извлечь из тэг <a> текст linkText
                        link_text = a_tag.text.lower()
                        self.addUrlToURLList(href_tag)
                        # добавить данные в linkBetweenURL
                        cursor.execute("""select rowId from URLList where url=%s""", [url])
                        from_url = cursor.fetchall()[0]
                        cursor.execute("""select rowId from URLList where url=%s""", [href_tag])
                        to_url = cursor.fetchall()[0]
                        cursor.execute("""insert  into linkBetweenURL(fk_FromURL_Id, fk_ToURL_Id) values(%s, %s);""", [from_url, to_url])
            # вызвать функцию класса Crawler для добавления содержимого в индекс
            #self.addToIndex(soup, url)

            # конец обработки текущ url
                    pass

        # конец обработки всех URL на данной глубине
        pass

        pass

    # 7. Инициализация таблиц в БД
    def initDB(self):
        cursor = self.conn.cursor()
        sql = 'create table wordList (rowId serial PRIMARY KEY,\
                                      word text,\
                                      isFiltred integer);'
        res = cursor.execute(sql)
        print('Создана таблица wordList')
        sql = 'create table URLList (rowId serial PRIMARY KEY,\
					                 url text);'
        cursor.execute(sql)
        print('Создана таблица URLList')
        sql = 'create table wordLocation (rowId serial PRIMARY KEY,\
                                          fk_wordId integer REFERENCES wordList(rowId),\
                                          fk_URLId integer REFERENCES URLList(rowId),\
                                          wordLocation integer);'
        cursor.execute(sql)
        print('Создана таблица wordLocation')
        sql = 'create table linkBetweenURL (rowId serial PRIMARY KEY,\
					                        fk_FromURL_Id integer REFERENCES URLList(rowId),\
						                    fk_ToURL_Id integer REFERENCES URLList(rowId));'
        cursor.execute(sql)
        print('Создана таблица linkBetweenURL')
        sql = 'create table linkWord (rowId serial PRIMARY KEY,\
                                      fk_wordId integer REFERENCES wordList(rowId),\
                                      fk_linkId integer REFERENCES linkBetweenURL(rowId));'
        cursor.execute(sql)
        print('Создана таблица linkWord')
        cursor.close()
    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value):
        return 1
