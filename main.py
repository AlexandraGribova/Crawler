import crawler
## Локальные переменные
dbFileName = 'Crawler'

crawler = crawler.Crawler(dbFileName)
crawler.initDB()
urlList = ['https://ngs.ru/']
crawler.crawl(urlList, 2)