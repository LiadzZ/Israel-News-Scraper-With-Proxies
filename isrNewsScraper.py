
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import ssl
from proxiesPool import proxiesPool
import json
from newspaper import Article
import time

client = MongoClient("mongodb+srv://Test1:Test1@cluster0-pwaip.mongodb.net/test?retryWrites=true&w=majority")
proxies_pool = None
headers_pool = None

def add_category(web, name, link):
    category_to_add = {
        "website": web,
        "name": name,
        "link": link
    }
    print("add_category Activated")
    with client:
        db = client.Categories
        categories = db.category
        categories.insert_one(category_to_add).inserted_id


# current_proxy = next(proxies_pool)
# current_headers = next(headers_pool)
def change_proxies(proxies_pool, headers_pool):
    print("change_proxies Activated")
    current_proxy = next(proxies_pool)
    current_headers = next(headers_pool)
    return current_proxy, current_headers


def load_page(link,proxies_pool=None, headers_pool=None):
    print("load_page Activated")
    flag = False
    page = None
    current_proxy = None
    current_headers = None
    if proxies_pool:
        current_proxy = next(proxies_pool)
        current_headers = next(headers_pool)
    while (not flag):
        with requests.Session() as res:
            try:
                if current_headers and current_proxy:
                    page = res.get(link, proxies={"http": current_proxy, "https": current_proxy},
                                   headers=current_headers, timeout=30)
                else:
                    page = res.get(link, timeout=30)
                flag = True
            except Exception as e:
                flag = False
                if current_headers and current_proxy:
                    current_proxy, current_headers = change_proxies(proxies_pool, headers_pool)
                    print("[System]:Error loading page ,", link, "message:", e)
                    print("[System]:Changing proxies")
                    time.sleep(5)
                else:
                    print("[System]:Error loading page ,", link, "message:", e)
                    print("[System]:Exit..try using proxies")
                    exit()


            if page:
                print("[System] Connected to :", link)
                return page

def newspaper_parser (newspaper,links,topic, sleep_time=2):
    print("newspaper_parser Activated")

    results = []
    count = 0
    #links = ['https://www.ynetnews.com/article/H1zKfsc9L']
    for l in links:
        article = Article(url=l)
        try:
            article.build()
        except Exception as e:
            print("Error 75:",e)
            time.sleep(60)
            continue



        data = {
            'title': article.title,
            'genre': topic,
            'date_published': article.publish_date,
            'news_outlet': newspaper,
            'authors': article.authors,
            'feature_img': article.top_image,
            'link': article.canonical_link,
            'keywords': article.keywords,
            'summary': article.summary,
            'text': article.text
            # 'movies': (article.movies).tolist(),
            #'html': article.html
        }
        print(data['title'])
        if count < 1: # print 1 article
            #print("data['title']")
            #print(data['title'])
            print("-----------------------------Article dit---------------------------")
            print("data['text']")
            print(data['date_published'])
            print(data['authors'])
            print(data['link'])
            print(data['keywords'])
            print(data['summary'])
            print("--------------------------------------------------------")
            print(data['text'])
            print("--------------------------------------------------------")
        # print
        # print

        if data['text']:
            with client:
                print("[System]: Adding n12 article to db")
                db = client.n12Articles
                articles = db.n12articles
                articles.insert_one(data)

        count += 1
        print(count)
        time.sleep(sleep_time)

    return results

def extract_n12_categories(page):
    print("extract_n12_categories Activated")
    soup = BeautifulSoup(page.content, 'html.parser')
    # print(soup.prettify())

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
            s = link.split('-')[1].split('?')[0]
            add_category('www.n12.co.il', s, link)
        return True
    else:
        print("Unable to extract data?")
        return False

def extract_n12_articles(page,topic):
    print("extract_n12_articles Activated")
    soup = BeautifulSoup(page.content, 'html.parser')
    #print(soup.prettify())



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
            first_try = "https://www.n12.co.il" + str(link)
            complete_links.append(first_try)

    newspaper_parser("n12",complete_links,topic)
    return True

def extract_ynet_categories(page):
    print("extract_ynet_categories Activated")
    soup = BeautifulSoup(page.content, 'html.parser')
    # print(soup.prettify())

    print("-----------------------")

    #sub_links = []
    data = soup.findAll('div', attrs={'class': 'MobileMenu'})
    for div in data:
        links = div.findAll('a')
        # print(links)
        for a in links:
            #sub_links.append(a['href'])
            link = str(a)
            link = link.split('"')[1]
            name = str(a)
            name = name.split(">")[2].split("<")[0]
            # print("[System]:1 ",link)
            # print("[System]:2 ", a)
            # print("[System]:3 ",name)
            if "http" in link:
                add_category('https://www.ynetnews.com/', name, link)
            #print("[System]:2 ",a['href'])


def extract_ynet_articles():
    print("extract_ynet_articles Activated")
    # Extract by genre
    total_articles = []
    categories_link = {}
    with client:
        db = client.Categories
        categories = db.category
        link = 'https://www.ynetnews.com/'
        for x in categories.find({"website": link}, {"_id": 0,"website": 1, "name": 1, "link": 1}):
            if ".com" in x['link']: # i dont want .co.il
                categories_link[x['name']] = x['link']

    for category_name in categories_link:
        print("[System]: ",category_name, "  ", categories_link[category_name])
        print("[System]: browsing ", category_name)
        page = load_page(categories_link[category_name])
        soup = BeautifulSoup(page.content, 'html.parser')
        # print(soup.prettify())

        print("-----------------------")

        sub_links = []

        for l in soup.findAll('a'):
            temp = str(l.get('href'))
            if "http" in temp and "article" in temp:
                sub_links.append(temp)
        # data = soup.findAll('a')
        # for div in data:
        #     l = div.get('href')
        #     print(l)
        #     if "article" in l:
        #         sub_links.append(l)
        #     #print(div)
        #     # links = div.findAll('a')
        #     # # print(links)
        #     # for a in links:

        sub_links = list(set(sub_links))
        # print("[System]: printing ",len(sub_links) ," sublinks:")
        # for l in sub_links:
        #     print(l)

        for article in sub_links:
            if article not in total_articles:
                total_articles.append(article)
            else:
                continue
            p = load_page(article)
            s = BeautifulSoup(p.content, 'html.parser')
            data = s.findAll('script', attrs={'type': 'application/ld+json'})
            data = str(data[0])
            data = data.split("{",1)[1]
            d = data.strip("</script>")
            d = "{" + d

            #print(d)
            try:
                k = json.loads(d)
            except Exception as e:
                print("[System]: Error at ",category_name," Error: ",e)
                exit()
            k['genre'] = category_name
            k['link'] = categories_link[category_name]

            with client:
                print("[System]: Adding article to db")
                db = client.ynetArticles
                articles = db.articles
                articles.insert_one(k)
            #print(k)
            # final_dictionary = eval(d)
            #
            # # printing final result
            # print("final dictionary", str(final_dictionary))
            # print("type of final_dictionary")
            #print(d)


def main():
    try:
        db = client.test
    except Exception as e:
        print("Could not connect to DB", e)
        exit()

    #p = proxiesPool()
    #current_proxy, current_headers = p.getProxies()

    # -----------------------------
    # Extracting ynet categories
    # -----------------------------

    # link = "https://www.ynetnews.com"
    # # page = load_page(link)
    # # handler = extract_ynet_categories(page)
    # extract_ynet_articles()  # done after categories already extracted

    # -----------------------------
    # Extracting ynet categories
    # -----------------------------

    link = "https://www.n12.co.il/"

    # # proxies_pool, headers_pool = p.create_pools()
    # # page = load_page(proxies_pool, headers_pool)  # with proxies
    # page = load_page()  # without proxies

    # -----------------------------
    # Extracting n12 categories
    # -----------------------------

    # while True:
    #     handler = extract_n12_categories(page)
    #     if handler == False:
    #         if proxies_pool:
    #             page = load_page(link,proxies_pool, headers_pool)
    #         else:
    #             print("[System]: Unable to extract n12 categories , try Using different proxy")
    #             break
    #             #page = load_page(link)
    #     else:
    #         break

    # -----------------------------
    # Extracting n12 categories
    # -----------------------------

    # ---------------------------------
    # Extracting n12 articles by categories # done after categories already extracted
    # ---------------------------------




    with client: # extract categories from mongo
        db = client.Categories
        categories = db.category
        categories_link = {}
        for x in categories.find({}, {"_id": 0,"website": 1, "name": 1, "link": 1}):
            categories_link[x['name']] = "http://" + x['website'] + x['link']
    for topic in categories_link:
        print("[System]: 1: ",topic, "  ", categories_link[topic])
        print("[System]: browsing ", topic)
        page = load_page(categories_link[topic])
        while True:
            handler = extract_n12_articles(page,topic)
            if handler == False:
                if proxies_pool:
                    page = load_page(link,proxies_pool, headers_pool)
                else:
                    print("[System]: Unable to extract n12 ",topic," category , try Using different proxy")
                    break
                    #page = load_page(link)
            else:
                break
        # break # stop after 1 iteration



    # ---------------------------------
    # Extracting n12 articles by categories
    # ---------------------------------
main()