import datetime  
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

from addresses_scraper import get_number_of_pages
from addresses_scraper import get_lst_of_adresses
from data_scraper import data_sheet




# Define a list of cities to scrape data for, choose one or more cities
list_of_cities = ["brno"] 

# Choose one property from the list [byty, domy, pozemky, komercni-nemovitosti, male-objekty-garaze]
property_type = "byty"

# Choose one or more actions from the list [prodej, pronajem, drazba]
actions = ["prodej", "pronajem"]  


load_dotenv()
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


template = {
    "Číslo zakázky": None,
    "Cena": None,
    "Dispozice": None,
    "Adresa": None,
    "Popis": None,
    "Konstrukce budovy": None,
    "Stav bytu": None,
    "Vlastnictví": None,
    "Přístupová komunikace": None,
    "Výstavba": None,
    "Užitná plocha": None,
    "Podlaží": None,
    "Počet podlaží budovy": None,
    "Lodžie": None,
    "Balkon": None,
    "Parkování": None,
    "Plyn": None,
    "Topení": None,
    "Elektřina": None,
    "Vybavení": None,
    "Občanská vybavenost": None,
    "Datum nastěhování": None,
    "Roční spotřeba energie": None,
    "PENB": None,
    "Stav budovy": None,
    "Terasa": None,
    "Plocha zahrady": None,
    "Dopravní dostupnost": None,
    "Čas uložení": datetime.datetime.now().strftime("%d.%m.%Y") 
}

if __name__ == "__main__":
    for city in list_of_cities:
        for action in actions:
            start_url = f"https://reality.idnes.cz/s/{action}/{property_type}/{city}/?page="
            number_of_pages = get_number_of_pages(start_url)
            addresses_list = get_lst_of_adresses(number_of_pages, start_url)
            data = data_sheet(addresses_list, template)

            df = pd.DataFrame(data)
            for col in df.columns:
               if df[col].dtype == "object":
                  df[col] = df[col].astype(str)

            df = df.apply(lambda col: col.apply(lambda x: None if x == "" else x))
            df = df.where(pd.notnull(df), None)

            engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
            df.to_sql(f"{city}_{property_type}_{action}", engine, if_exists = "append", index = False)
            print(f"{city}_{property_type}_{action} saved")
