import bs4
import psycopg2
import pymorphy2
import requests


class Crawler:
    # Словарь со ссылками и количеством повторений
    dictionaryURL = {'https://ngs.ru/': 1}
    # Словарь со словами и количеством повторений
    dictionaryWords = {}

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
        sql = 'select \'create database %s\' where NOT EXISTS (SELECT FROM pg_database WHERE datname = \'%s\')' % (
            db_name, db_name)
        cursor.execute(sql)  ## Если БД уже существует, то ничего не вернет, иначе вернет 'ctreate db name'
        res = cursor.fetchall()
        if len(res) != 0:
            cursor.execute(res)
        print('Создана БД Crawler')
        ## Удаляем все таблицы\
        sql = 'drop table if exists linkWord;\
                       drop table if exists linkBetweenURL;\
                       drop table if exists wordLocation;\
                       drop table if exists URLList;\
                       drop table if exists wordList;'
        cursor.execute(sql)
        print('Все таблицы удалены')
        cursor.close()
        pass

    # 0. Деструктор
    def __del__(self):
        self.conn.close()
        pass

    # 1. Индексирование одной страницы
    # вместо url передаем айдишник данной ссылки в URLList
    def addIndex(self, soup, url):
        cursor = self.conn.cursor()
        word_counter = 0
        if self.isIndexed(url):
            # Если страница уже есть в базе обработка происходить не будет
            print('Уже обработана')
            return
        else:
            # Если страницы нет в базе подготавливаем и добавляем
            # Получаем текст
            text = self.getTextOnly(soup)
            # Чистим текст
            wordList = self.separateWords(text)
            # Добавляем слово, если не существует
            for word in wordList:
                rowId = self.getEntryId(word)
                cursor.execute(
                    """insert into wordLocation(fk_wordid, fk_urlid, wordlocation) values(%s, %s, %s) returning *""",
                    [rowId, url, word_counter])
                word_counter += 1

    # 2. Получение текста страницы
    def getTextOnly(self, soup):
        text = soup.get_text()
        return text

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        # Добавляем словарь знаков препинания
        punctuationMarkDict = ['.', ',', '!', '?', ':', ';', '...', '"', '-', '(', ')']
        # Добавляем выходной словарь
        outDict = []
        # Объявляем функцию, которая будет очищать текст от союзов
        morph = pymorphy2.MorphAnalyzer(lang='ru')
        # Массив для очистки текста от знаков препинания
        for punctuationMark in punctuationMarkDict:
            # Убираем каждый знак препинания из текста
            text = text.replace(punctuationMark, '')
        # Массив для очистки текста от союзов
        for s in text.split():
            # Определяем часть речи
            tag = morph.parse(s)[0].tag.POS
            if tag != 'CONJ':
                # Если не союз добавляем слово в выходной список
                outDict.append(s)
        return outDict

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        cursor = self.conn.cursor()
        cursor.execute("""select * from wordLocation where fk_URLid = %s""", [url])
        result = cursor.fetchall()
        if len(result) == 0:
            return 0
        else:
            return 1

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        cursor = self.conn.cursor()
        # добавить данные в linkBetweenURL
        cursor.execute("""select rowId from URLList where url=%s""", [urlFrom])
        from_url = cursor.fetchall()[0]
        cursor.execute("""select rowId from URLList where url=%s""", [urlTo])
        to_url = cursor.fetchall()[0]
        cursor.execute("""insert  into linkBetweenURL(fk_FromURL_Id, fk_ToURL_Id) values(%s, %s) returning *;""",
                       [from_url, to_url])
        link_word_id = cursor.fetchall()[0][0]
        # парсим linkText
        for s in linkText.split():
            # помещаем его в wordList
            rowId = self.getEntryId(s)
            # заполянем linkWord
            cursor.execute("""insert into linkWord(fk_wordId, fk_linkid) values(%s, %s) returning *""",
                           [rowId, link_word_id])
        return from_url

    # Добавление ссылки в URLList с проверкой на наличие дублей
    def addUrlToURLList(self, url):
        cursor = self.conn.cursor()
        cursor.execute("""select * from URLList where url=%s""", [url])
        result = cursor.fetchall()
        #   добавить в таблицу URLList если такой записи нет
        if len(result) == 0:
            # Если ссылки нет в словаре добавляем и делаем значение 1
            self.dictionaryURL[url] = 1
            cursor.execute("""insert  into URLList(url) values(%s);""", [url])
        else:
            # Если ссылка есть в словаре прибавляем к значению 1
            self.dictionaryURL[url] += 1
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
            flag = False
            for url in urlList:
                # получить HTML-код страницы по текущему url
                html_doc = requests.get(url).text
                # использовать парсер для работы тегов
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                # получить список тэгов <a> с текущей страницы
                for a_tag in soup.findAll('a'):
                    #   проверить наличие атрибута 'href'
                    href_tag = a_tag.get('href')
                    #   убрать пустые ссылки, вырезать якоря из ссылок, и т.д.
                    if href_tag is not None and not '#' in href_tag and href_tag != '/':  # !! вот тут непонятные локальые ссылки не удаляю пока что
                        if not 'http' in href_tag:
                            href_tag = url[:-1] + href_tag
                        #   добавить ссылку в список следующих на обход
                        urlListNew.append(href_tag)
                        #   извлечь из тэг <a> текст linkText
                        link_text = a_tag.text.lower()
                        self.addUrlToURLList(href_tag)
                        from_url = self.addLinkRef(url, href_tag, link_text)
                        cursor.execute('select count(*) from urllist')
                        resultNow = cursor.fetchone()[0]
                        if resultNow % 10 == 0:
                            print('/-----------------------------------/')
                            self.printValues()
                        if resultNow == 150:
                            flag = True
                            break
                # вызвать функцию класса Crawler для добавления содержимого в индекс
                self.addIndex(soup, from_url)
                # конец обработки текущ url
                if flag:
                    break
                pass
            # конец обработки всех URL на данной глубине
            pass
        print('/-----------------------------------/')
        print('Первые 20 наиболее часто проиндексированных ссылок:')
        self.dictSort(self.dictionaryURL)
        print('/-----------------------------------/')
        print('20 наиболее часто встречающихся слов на страницах:')
        self.dictSort(self.dictionaryWords)
        print('/-----------------------------------/')
        print('Количество строк в таблицах:')
        self.printValues()
        pass

    # Функция для сортировки словаря и вывода значений
    def dictSort(self, dictionary):
        # Сортируем словарь
        dictionary = dict(sorted(dictionary.items(), key=lambda item: item[1], reverse=True))
        # Добавляем счётчик
        counter = 0
        # Цикл для прохода по словарю и выводу значений
        for i in dictionary.keys():
            if dictionary.get(i) == 1:
                print(i, dictionary.get(i), sep=' - ', end=' time\n')
            else:
                print(i, dictionary.get(i), sep=' - ', end=' times\n')
            counter += 1
            # Если счётчик равен 20 завершаем цикл
            if counter == 20:
                break

    # Функция получения количества строк
    def printValues(self):
        cursor = self.conn.cursor()
        # Получение количества строк из таблицы wordlist
        cursor.execute('select max(rowid) from wordlist')
        result = cursor.fetchone()[0]
        print('Количество строк в таблице wordlist равно - ', result)
        # Получение количества строк из таблицы urllist
        cursor.execute('select max(rowid) from urllist')
        result = cursor.fetchone()[0]
        print('Количество строк в таблице urllist равно - ', result)
        # Получение количества строк из таблицы wordlocation
        cursor.execute('select max(rowid) from wordlocation')
        result = cursor.fetchone()[0]
        if result is None:
            print('Количество строк в таблице wordlocation равно - 0')
        else:
            print('Количество строк в таблице wordlocation равно - ', result)
        # Получение количества строк из таблицы linkbetweenurl
        cursor.execute('select max(rowid) from linkbetweenurl')
        result = cursor.fetchone()[0]
        print('Количество строк в таблице linkbetweenurl равно - ', result)
        # Получение количества строк из таблицы linkword
        cursor.execute('select max(rowid) from linkword')
        result = cursor.fetchone()[0]
        print('Количество строк в таблице linkword равно - ', result)

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
    def getEntryId(self, value):
        cursor = self.conn.cursor()
        # если в таблице wordList нет слова, то добавляем
        cursor.execute("""select rowId from wordList where word = %s""", [value])
        resultSelect = cursor.fetchone()
        if resultSelect is None:
            cursor.execute("""insert into wordList(word, isFiltred) values(%s, '0') returning *""", [value])
            # Если слова нет в словаре добавляем его в словарь и делаем значение 1
            self.dictionaryWords[value] = 1
            return cursor.fetchall()[0][0]
        else:
            # Если слово есть в словаре прибавляем к его значению 1
            self.dictionaryWords[value] += 1
            return resultSelect[0]
