import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import ssl
from proxiesPool import proxiesPool
import json
from newspaper import Article
import time
from dateutil.parser import parse
import csv
import datetime


class Handler:
    def __init__(self):
        #self.client = MongoClient("EnterYourMongoDB-Here")
        #self.client = MongoClient("mongodb+srv://")
        self.current_proxy = None
        self.current_headers = None
        self.p = None
        self.dateStart = None
        self.dateEnd = None
        self.test_db()

    def test_db(self):
        try:
            db = self.client.test
        except Exception as e:
            print("Could not connect to DB", e)
            exit()

    def init_proxies(self):
        self.p = proxiesPool()
        self.current_proxy, self.current_headers = self.p.getProxies()

    def add_category(self,web, name, link):
        category_to_add = {
            "website": web,
            "name": name,
            "link": link
        }
        print("[System]: add_category Activated")
        if "n12" in web:
            with self.client:
                db = self.client.N12Categories
                categories = db.category
                categories.insert_one(category_to_add).inserted_id
        elif "ynet" in web:
            with self.client:
                db = self.client.ynetCategories
                categories = db.category
                categories.insert_one(category_to_add).inserted_id

    def add_article(self,newspaper, data):
        print("[System]: add_article Activated")
        if newspaper == "n12":
            with self.client:
                print("[System]: Adding n12 article to db")
                db = self.client.n12Articles
                articles = db.n12articles
                articles.insert_one(data)
        elif newspaper == "ynet":
            with self.client:
                print("[System]: Adding ynet article to db")
                db = self.client.ynetArticles
                articles = db.ynetarticles
                articles.insert_one(data)

    def write_to_csv (self, data, file_name):
        print('writing to CSV...')

        keys = data[0].keys()
        with open(file_name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)

    def load_page(self,link):
        print("[System]: load_page Activated")
        flag = False
        page = None
        attempts = 0
        # current_proxy = None
        # current_headers = None
        if not self.current_proxy:
            current_proxy, current_headers = self.p.changeProxies()
        else:
            current_proxy, current_headers = self.current_proxy ,self.current_headers
        print("[System]: current_proxy:",current_proxy)
        #print("current_headers:", current_headers)
        #current_proxy = None
        while (not flag):
            attempts+=1
            if attempts > 10:
                print("[System]: init proxies")
                #current_proxy, current_headers = self.p.getProxies()
                self.init_proxies()
            if attempts > 20:
                print("[System]: Tried 20 times to connect and failed.Abort this page")
                return False
            with requests.Session() as res:
                try:
                    if current_headers and current_proxy:
                        print("[System]: trying Proxy connection to ",link)
                        page = res.get(link, proxies={"http": current_proxy, "https": current_proxy},
                                       headers=current_headers, timeout=30)
                    else:
                        print("[System]: trying Normal connection to ",link)
                        page = res.get(link, timeout=30)
                    if page:
                        flag = True
                        self.current_proxy = current_proxy
                        self.current_headers = current_headers
                except Exception as e:
                    flag = False
                    if current_headers and current_proxy:
                        print("[System]: Error loading page ,", link, "message:", e)
                        print("[System]: Changing proxies")
                        time.sleep(5)
                        current_proxy, current_headers = self.p.changeProxies()
                        print("[System]: current_proxy:", current_proxy)
                    else:
                        print("[System]: Error loading page ,", link, "message:", e)
                        print("[System]: Exit..try using proxies")
                        exit()

        if page:
            print("[System] Connected to :", link)
            return page

    def check_dates (self, date):
        print("[System]: Checking date...")
        page_date = parse(date)
        if page_date >= self.dateStart and page_date <= self.dateEnd:
            #print("Date is fine..")
            return True
        return False

    def findN12Authors(self,link):
        print("[System]: findN12Authors Activated")
        page = self.load_page(link)
        soup = BeautifulSoup(page.content, 'html.parser')
        data = soup.find('script', attrs={'type': 'application/ld+json'})
        data = str(data)
        data = data.split("{", 1)[1]
        d = data.strip("</script>")
        d = "{" + d
        try:
            k = json.loads(d)
        except Exception as e:
            print("[System]: Error loading article meta-data to json " , e)
            return None
        return k['author']['name']
        # #print(data)
        # data = str(data)
        # data = data.split('>')[1]
        # #print(data)
        # data = data.split('<')[0]
        # print(data)
        # data = json(str(data))
        # print("Yes --------")
        # print(data)

    def newspaper_parser(self,newspaper, links, topic, sleep_time=2):
        print("[System]: newspaper_parser Activated")

        results = []
        count = 0
        # links = ['https://www.ynetnews.com/article/H1zKfsc9L']
        for l in links:
            article = Article(url=l)
            try:
                article.build()
            except Exception as e:
                print("Error 75:", e)
                time.sleep(10)
                continue
            date = article.publish_date.strftime("%d/%m/%Y")
            if self.dateStart:
                if self.check_dates(date):
                    print("[System]: date is ok")
                else:
                    print("[System]: date is out of range")
                    continue
            if newspaper == "n12":
                authors = self.findN12Authors(l)
            else:
                authors = article.authors
            data = {
                'title': article.title,
                'genre': topic,
                'date_published': date,
                'news_outlet': newspaper,
                'authors': authors,
                'feature_img': article.top_image,
                'link': article.canonical_link,
                'keywords': article.keywords,
                'summary': article.summary,
                'text': article.text
                # 'movies': (article.movies).tolist(),
                # 'html': article.html
            }
            print("title:",data['title'])

            if count < 1:  # print 1 article
                # print("data['title']")
                # print(data['title'])
                print("-----------------------------Article dit---------------------------")
                print("date_published:",data['date_published'])
                print("genre:", data['genre'])
                print("authors:",data['authors'])
                print("link:",data['link'])
                print("keywords:",data['keywords'])
                print("summary:",data['summary'])
                print("--------------------------------------------------------")
                print("text:",data['text'])
                print("--------------------------------------------------------")

            # print
            # print

            if data['text']:
                self.add_article(newspaper,data)

            count += 1
            print(count)
            time.sleep(sleep_time)

        return results




class N12Scraper():
    def __init__(self, handler):
        self.link = "www.n12.co.il"
        self.handler = handler

    def get_categories(self):
        with self.handler.client:  # extract categories from mongo
            db = self.handler.client.N12Categories
            categories = db.category
            categories_link = {}
            for x in categories.find({"website": self.link}, {"_id": 0, "website": 1, "name": 1, "link": 1}):
                categories_link[x['name']] = "http://" + x['website'] + x['link']
            return categories_link
            # for topic in categories_link:
            #     print("[System]: 1: ", topic, "  ", categories_link[topic])

    def extract_n12_categories_links(self, page):
        print("extract_n12_categories Activated")
        soup = BeautifulSoup(page.content, 'html.parser')
        print(soup.prettify())
        print("-----------------------")
        sub_links = []
        data = soup.findAll('li', attrs={'class': 'more'})
        for div in data:
            links = div.findAll('a')
            # print(links)
            for a in links:
                sub_links.append(a['href'])
                # print(a['href'])

        if sub_links:
            for link in sub_links:
                link = link.split('?')[0]
                #print("link:",link)
                if "/news-" in link:
                    s = link.split('-')[1].split('?')[0]
                    self.handler.add_category('www.n12.co.il', s, link)
            return True
        else:
            print("Unable to extract data?")
            return False

    def extract_n12_articles_links(self, page, topic):
        print("extract_n12_articles_links Activated")
        soup = BeautifulSoup(page.content, 'html.parser')
        # print(soup.prettify())

        sub_links = []
        data = soup.findAll('ul', attrs={'class': 'grid-ordering'})
        for div in data:
            links = div.findAll('a')
            for a in links:
                sub_links.append(a['href'])
        data = soup.findAll('section', attrs={'class': 'content'})
        for div in data:
            links = div.findAll('a')
            for a in links:
                sub_links.append(a['href'])
        sub_links = set(sub_links)
        complete_links = []
        if sub_links:
            for link in sub_links:
                full_link = "https://www.n12.co.il" + str(link)
                complete_links.append(full_link)

            self.handler.newspaper_parser("n12", complete_links, topic)
        else:
            return False
        return True

    def extract_n12_categories(self,link=None):
        if not link:
            link = "https://www.n12.co.il/"
        flag = False
        while flag is False:
            page = self.handler.load_page(link)
            flag = self.extract_n12_categories_links(page)

            if flag is False:
                print("[System]: Unable to extract n12 categories")

    def extract_n12_articles(self, startDate, endDate,topic='politics',num_of_pages = 50):
        if startDate:
            self.handler.dateStart = startDate
            self.handler.dateEnd = endDate
        categories_link = self.get_categories()
        for x in range(num_of_pages):
            flag = False
            link = categories_link[topic] + "?page=" + str(x+1)
            print("[System]: Browsing,"+link)
            if link:
                while flag is False:
                    page = self.handler.load_page(link)
                    flag = self.extract_n12_articles_links(page, topic)
                    if flag is False:
                        print("[System]: Unable to extract n12 articles")
            else:
                print("[System]: no categories found")


class YnetScraper():
    def __init__(self,handler):
        self.link = "www.ynet.co.il"
        self.handler = handler

    def get_categories(self):
        print("[System]: get_categories Activated (Ynet)")
        with self.handler.client:  # extract categories from mongo
            db = self.handler.client.ynetCategories
            categories = db.category
            categories_link = {}
            for x in categories.find({"website": self.link}, {"_id": 0, "website": 1, "name": 1, "link": 1}):
                categories_link[x['name']] = x['link']
            return categories_link

    def filter_articles(self,current_links):
        print("[System]: filter_articles Activated (YnetScraper)")
        with self.handler.client:  # extract categories from mongo
            db = self.handler.client.ynetArticles
            articles = db.ynetarticles
            articles_link = []
            for x in articles.find({"news_outlet": "ynet"}, {"link": 1}):
                articles_link.append(x['link'])
        filtered_articles = [x for x in current_links if x[0] not in articles_link]
        print("[System]: filtered ",len(articles_link) , " articles that are already in DB")
        return filtered_articles


    def extract_ynet_categories(self):
        with open("ynetCategories.txt", "r") as text:
            for line in text:
                x = line.split()
                self.handler.add_category('www.ynet.co.il', x[1], x[0])

    def fix_date(self, str):

        try:
            date_time_obj = datetime.datetime.strptime(str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception:
            pass
        try:
            date_time_obj = datetime.datetime.strptime(str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception:
            pass
        if  date_time_obj:
            return date_time_obj.date().strftime("%d/%m/%Y")
        else:
            print("[System]: ynet fix date did not work.")
            return str

    def ynet_parser(self,newspaper,complete_links, topic):
        print("[System]: ynet_parser Activated")
        total = 0
        added = 0
        for link in complete_links:
            total += 1
            page = self.handler.load_page(link)
            if page == False:
                continue
            soup = BeautifulSoup(page.content, 'html.parser')
            data = soup.find('script', attrs={'type': 'application/ld+json'})
            data = str(data)
            data = data.split("{", 1)[1]
            d = data.strip("</script>")
            d = "{" + d
            try:
                metaData = json.loads(d)
            except Exception as e:
                print("[System]: Error loading article meta-data to json ", e)
                continue

            authors = metaData['author']['name'].split(',') # to list

            keywords = metaData['keywords'].split(',') # to list

            date = self.fix_date(metaData['datePublished'])

            data = {
                'title': metaData['headline'],
                'genre': topic,
                'date_published': date,
                'news_outlet': newspaper,
                'authors': authors,
                #'feature_img': article.top_image,
                'link': link,
                'keywords': keywords,
                'summary': metaData['description'],
                'text': metaData['articleBody']
                # 'movies': (article.movies).tolist(),
                # 'html': article.html
            }
            if data['text']:
                added+=1
                self.handler.add_article(newspaper,data)
                print("[System]: Added Articles:", added)
        print("[System]: Total Articles:",total)
        print("[System]: Added Articles:", added)

    def extract_ynet_articles_links(self,page,topic):
        print("[System]: extract_ynet_articles_links Activated")
        soup = BeautifulSoup(page.content, 'html.parser')
        # print(soup.prettify())

        sub_links = []
        for a in soup.find_all('a', href=True):
            if "article" in str(a['href']):
                sub_links.append(a['href'])

        sub_links = set(sub_links)

        complete_links = []
        if sub_links:
            for link in sub_links:
                full_link = "https://www.ynet.co.il" + str(link)
                complete_links.append(full_link)

            #self.handler.newspaper_parser("ynet", complete_links, topic)
            complete_links = self.filter_articles(complete_links)
            self.ynet_parser("ynet",complete_links, topic)
        else:
            return False
        return True

        #
        # data = soup.findAll('ul', attrs={'class': 'grid-ordering'})
        # for div in data:
        #     links = div.findAll('a')
        #     for a in links:
        #         sub_links.append(a['href'])
        # data = soup.findAll('section', attrs={'class': 'content'})
        # for div in data:
        #     links = div.findAll('a')
        #     for a in links:
        #         sub_links.append(a['href'])
        # sub_links = set(sub_links)
        # complete_links = []
        # if sub_links:
        #     for link in sub_links:
        #         full_link = "https://www.n12.co.il" + str(link)
        #         complete_links.append(full_link)
        #
        #     self.handler.newspaper_parser("n12", complete_links, topic)
        # else:
        #     return False
        # return True

    def extract_ynet_articles(self,startDate, endDate,topic='politics-internal'):
        if startDate:
            self.handler.dateStart = startDate
            self.handler.dateEnd = endDate
        categories_link = self.get_categories()
        flag = False
        link = categories_link[topic]
        print("[System]: Browsing," + link)
        if link:
            while flag is False:
                page = self.handler.load_page(link)
                flag = self.extract_ynet_articles_links(page, topic)
                if flag is False:
                    print("[System]: Unable to extract ynet articles")
        else:
            print("[System]: no categories found")

hand = Handler()
hand.init_proxies()
scrape = YnetScraper(hand)
#scrape.extract_ynet_categories()
scrape.extract_ynet_articles(startDate=None,endDate=None)


#scrape = N12Scraper(hand)
#scrape.extract_n12_categories()
#scrape.extract_n12_articles(startDate=None,endDate=None,topic='politics',num_of_pages = 5)
