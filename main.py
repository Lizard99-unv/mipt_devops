from bs4 import BeautifulSoup as bs
import requests
from requests_html import HTMLSession
from tqdm.auto import tqdm
import json
import logging
from multiprocessing import Pool, Queue

def get_listing(page_num):
    host = 'https://gg.deals'
    listing = '/games'
    url = host + listing
    session = HTMLSession()
    for _ in range(3):
        page = session.get(url, params={'sort': 'metascore', 'type': '1', 'page': page_num}, timeout=20)
        if page.status_code == 200:
            break
    if page.status_code != 200:
        logging.error("Failed to parse! ", url)
        return []
    page = list(map(lambda x: host + x.attrs['href'], page.html.find('.game-info-title')))
    return page

def get_page(url):
    game = {}
    game['url'] = url

    session = HTMLSession()
    for _ in range(3):
        page = session.get(url, timeout=20)
        if page.status_code == 200:
            break
    if page.status_code != 200:
        logging.error("Failed to parse! ", url)
        return None
    html = page.html
    game['title'] = html.find('.breadcrumbs-list')[0].find('span')[-1].text
    game['official_store_price'] = html.find('.game-info-price-col')[0].find('span')[1].text
    game['keyshop_price'] = html.find('.game-info-price-col')[1].find('span')[1].text
    game['img'] = html.find('.game-info-image-wrapper')[0].find('img')[0].attrs['src']

    widget = html.find('.game-info-widget')[0]
    info = widget.find('.game-info-details-content')
    if len(info) > 0:
        game['release_date'] = info[0].text
    if len(info) > 1:
        game['developer'] = info[1].text

    if len(info) > 2:
        reviews = info[2].find('span')
    else:
        reviews = []
    if len(reviews) > 0:
        game['metascore'] = reviews[0].text
    if len(reviews) > 1:
        game['user_score'] = reviews[1].text
    if len(reviews) > 2:
        game['review'] = reviews[2].text

    genres = widget.find('.game-info-genres')
    if len(genres) > 0:
        game['genres'] = list(map(lambda x: x.text, genres[0].find('a')))
    tags = widget.find('.game-info-tags')
    if len(tags) > 0:
        game['tags'] = list(map(lambda x: x.text, tags[0].find('a')))
    if len(tags) > 1:
        game['features'] = list(map(lambda x: x.text,tags[1].find('a')))
    platforms = widget.find('.game-info-details-section')
    if len(platforms) > 1:
        game['platforms'] = list(map(lambda x: x.attrs['title'], platforms[1].find('svg')))

    info = html.find('.game-info-actions')[0]
    info = list(map(lambda x: x.text, info.find('.count')[::2]))
    if len(info) > 0:
        game['wishlist_count'] = info[0]
    if len(info) > 1:
        game['alert_count'] = info[1]
    if len(info) > 2:
        game['owners_count'] = info[2]

    shops = html.find('.tab-menu-section')
    offers = list(filter(lambda x: x.attrs['id'] == 'offers', shops))
    if len(offers) > 0:
        offers = offers[0].find('.offer-section')
        if len(offers) > 0:
            game['official_shops'] = list(offers[0].absolute_links)
        if len(offers) > 1:
            game['keyshops'] = list(offers[1].absolute_links)
    dlcs = list(filter(lambda x: x.attrs['id'] == 'game-dlcs', shops))
    if len(dlcs) > 0:
        dlcs = dlcs[0].find('.full-link')
        if len(dlcs) > 0:
            game['dlcs'] = list(dlcs[0].absolute_links)
    packs = list(filter(lambda x: x.attrs['id'] == 'game-packs', shops))
    if len(packs) > 0:
        game['packs'] = list(packs[0].absolute_links)

    return game

def main():
    logging.basicConfig(filename="event.log", level=logging.INFO, filemode="w")
    print("How many pages would you like to parse?")
    page_num = int(input())
    
    urls = []
    print("Listings parsing:")
    for num in tqdm(range(1, page_num + 1)):
        urls += get_listing(num)
    print("Listings parsing completed!")
    
    print("Pages parsing:")
    parsed = []
    with Pool(16) as pool:
        for page in tqdm(pool.imap_unordered(get_page, urls)):
            parsed.append(page)
    print("Pages parsing completed!")
    with open('result.json', 'w') as fp:
        for page in parsed:
            json.dump(page, fp)
    return
    
if __name__=="__main__":
    main()