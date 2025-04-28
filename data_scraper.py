from bs4 import BeautifulSoup  
import asyncio 
import aiohttp

from selector import CLASS_HTML_FOR_LAYOUT_CLASS, CLASS_HTML_FOR_ADDRESS_CLASS
from selector import CLASS_HTML_FOR_DESCRIPTION_CLASS, CLASS_HTML_WITH_PARAMETERS_CLASS

def extract_values(class_html_with_parameters):
    all_dd = class_html_with_parameters.find_all("dd")
    text_dd = [item.text for item in all_dd]
    dd_clear = [item.replace("\t","") for item in text_dd]
    apartment_values = [item.replace("\n","") for item in dd_clear]

    # Correcting non-standard data formatting in the HTML
    if "Kč" in apartment_values[1]:                                 
        cut_point = apartment_values[1].index("Kč")
        apartment_values[1] = apartment_values[1][:cut_point]
    
    if "Kč" in apartment_values[0]:                                
        cut_point = apartment_values[0].index("Kč")
        apartment_values[0] = apartment_values[0][:cut_point]

    return apartment_values

def extract_indexes(class_html_with_parameters):
    all_dt = class_html_with_parameters.find_all("dt")
    apartment_indexes = [item.text for item in all_dt]

    return apartment_indexes

def extract_layout (doc):
    class_html_for_layout = doc.find("h1", class_= CLASS_HTML_FOR_LAYOUT_CLASS).text
    layout = class_html_for_layout.split()[2]

    return layout

def extract_address(doc):
    class_html_for_address = doc.find("p", class_= CLASS_HTML_FOR_ADDRESS_CLASS).text
    address = class_html_for_address.strip()

    return address

def extract_description(doc):
    class_html_for_description = doc.find("div", class_= CLASS_HTML_FOR_DESCRIPTION_CLASS)
    description = class_html_for_description.text.strip()
    description = " ".join(description.split())

    return description

async def fetch_data(session, url, queue, framework):
    """
    Processing the url and extracting all page data from it.
    """

    async with session.get(url) as response:
        html = await response.text()
        doc = BeautifulSoup(html, "html.parser")
        class_html_with_parameters = doc.find("div", class_= CLASS_HTML_WITH_PARAMETERS_CLASS)

        apartment_values = extract_values(class_html_with_parameters)
        apartment_indexes = extract_indexes(class_html_with_parameters)

        values_dict = dict(zip(apartment_indexes, apartment_values))

        temp = framework.copy()
        for key in temp:
            if key in values_dict:
                temp[key] = values_dict[key]

        if "Dispozice" in temp:
            temp["Dispozice"] = extract_layout(doc)

        if "Adresa" in temp:
            temp["Adresa"] = extract_address(doc)

        if "Popis" in temp:
            temp["Popis"] = extract_description(doc)

        await queue.put(temp)

    return temp

async def result_addresses(addresses_list, framework):
    """
    Creates an asynchronous task for each real estate url.
    """

    data = []

    async with aiohttp.ClientSession() as session:
        queue = asyncio.Queue()
        tasks = []
        for url in addresses_list: 
            task = asyncio.create_task(fetch_data(session, url, queue, framework))
            tasks.append(task)

        await asyncio.gather(*tasks)

        while not queue.empty():
            data.append(await queue.get())

    return data

def data_sheet(addresses_list, framework):
    """
    Starts an asynchronous search for individual page data and returns a list of them.
    """

    loop = asyncio.get_event_loop()
    output = loop.run_until_complete(result_addresses(addresses_list, framework))

    return output
