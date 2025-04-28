from bs4 import BeautifulSoup  
import requests  
import asyncio  
import aiohttp 

from selector import NUMBER_OF_OFFERS_STR_CLASS, ARTICLE_TAGS_CLASS

def get_number_of_pages(start_url): 
    """
    Function to determine the number of pages with offers
    """

    result_url = requests.get(start_url)
    doc_url = BeautifulSoup(result_url.text, "html.parser")

    number_of_offers_str = doc_url.find("p", class_= NUMBER_OF_OFFERS_STR_CLASS).text
    number_of_offers = int(number_of_offers_str.split()[0])

    if (number_of_offers % 20) == 0:        # 20 is the maximum number of properties on the page
        return number_of_offers // 20
    else:
        return number_of_offers // 20 + 1
    
def get_lst_of_adresses(number_of_pages, start_url):
    """
    Starts an asynchronous search for url addresses and returns a list of them.
    """

    loop = asyncio.get_event_loop()
    lst_of_adresses = loop.run_until_complete(result_addresses(number_of_pages, start_url))

    return lst_of_adresses

async def fetch_urls(session, url, queue):
    """
    Processing the page and extracting all page URLs from it.
    """

    async with session.get(url) as response:
        html = await response.text()
        doc = BeautifulSoup(html, "html.parser")
        article_tags = doc.find_all("a",  class_= ARTICLE_TAGS_CLASS)

        for tag in article_tags:
            await queue.put(tag["href"])

async def result_addresses(number_of_pages, start_url):
    """
    Creates an asynchronous task for each real estate page.
    """

    addresses = []

    async with aiohttp.ClientSession() as session:
        queue = asyncio.Queue()
        tasks = []

        for i in range(number_of_pages): 
            url = start_url + str(i)
            task = asyncio.create_task(fetch_urls(session, url, queue))
            tasks.append(task)

        await asyncio.gather(*tasks)
        
        while not queue.empty():
            addresses.append(await queue.get())

    return addresses


