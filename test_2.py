import concurrent.futures
# from bs4 import BeautifulSoup
import time
import re

# Importing data structures
from packets.websiteds import Website
from packets.dataStructures import TheAuthour

from packets.TheMiner import TheMiner, rupee as rate
from packets import dbmanip as db
from packets.global_vars import SELLER_INFO, ARTIST_INFO, visited, KEY_INFO


class Saatchiart:
    def __init__(self, website):
        self.website = website
        self.initial_listings = []
        self.artwork_listings = []

    def link_maker(self, rel_url):
        url = self.website.domain + rel_url
        return url


    #___________LISTINGS_______________________


    #_______________ARTIST_____________________________________

    def get_artist_id(self, artist_url):
        # We go to artist page to pick data we need to make the ARTIST_INFO key.

        artist_id = None
        if artist_url in KEY_INFO.keys():
            key = KEY_INFO[artist_url]
            artist_id = ARTIST_INFO[key]
        else:
            self.get_artist_data(artist_url)
            if artist_url in KEY_INFO.keys():
                key = KEY_INFO[artist_url]
                artist_id = ARTIST_INFO[key]
            else:
                # If it ever comes to here, the page will not have an Artist
                print("FATAL ERROR :: Artist_id not found. Artist_url broken or artist page may be down.")
            # Let's return None here, and not pick rest of the data if the artist_id is not found.
            # Artist id is used in artworks table only.

        return artist_id

    def get_artist_data(self, url):
        soup = TheMiner.fetch_page(url)
        if soup is not None:
            name = None
            born = None
            country = None
            about = None

            # name
            try:
                name = soup.find('h1').text.strip()
                # print(name)
            except AttributeError:
                pass
            except TypeError:
                pass

            if name is not None:
                # country
                try:
                    country = soup.find('div', class_='krw7aj-0 sc-3qpvhh-21 hDfspS eignsO').text.strip().split(",")
                    # print(country)
                    country = country[-1].strip()
                    # print(country)
                    # if len(country) > 20:
                    #     country.strip("  ")
                    #     t = ""
                    #     for i in country:
                    #         if i != "":
                    #             t += i
                    #             t += " "
                    #     country = t
                    # print(country)
                except AttributeError:
                    pass

                try:
                    about = soup.find_all('h3', class_='krw7aj-4 YUizi')
                    about = about[0].nextSibling.next.text
                    # print(about)
                except AttributeError:
                    pass

            artist_data_pack = [name, born, country, about]
            # pack = [name, born, country, about]
            # Updating KEY_INFO dictionary.
            KEY_INFO[url] = db.Artist.key_maker(artist_data_pack)
            # Updating the dB with artist listings.
            TheAuthour.write_artist(*artist_data_pack)
#__________________SELLER___________________________________

    def get_seller_id(self, seller_url) -> int:
        # Fetches seller_data, writes it in db, and returns seller_id.
        # bundle = [seller_url, self.website.platform, 'KAZoART', None, url]
        seller_id = None

        if seller_url is not None:
            if seller_url in SELLER_INFO.keys():
                seller_id = SELLER_INFO.get(seller_url)
                # print(seller_id)
            else:
                self.get_seller_data(seller_url)
                if seller_url in SELLER_INFO.keys():
                    seller_id = SELLER_INFO.get(seller_url)
                else:
                    print("FATAL ERROR :: Seller_id not found.")
        else:
            print("FATAL ERROR :: Seller_id not found.")
        # Let's return seller_id, even if it's None. This will stop the get_artwork_Data_slave from gathering
        # data beyond rule 3 check .
        return seller_id

    def get_seller_data(self, seller_url):
        # Caller :: get_artwork_data_slave and get_seller_id
        # bundle = [seller_url, platform, Seller's name, location, website]
        bundle = [seller_url, self.website.platform, self.website.platform, None, seller_url]
        # Writing to db.
        TheAuthour.write_seller(*bundle)

        # ________________________________________


#____________________ARTWORK_________________________________

    def get_artwork_listings(self):

        def recurr(url):
            soup = TheMiner.fetch_page(url)
            if soup is not None:
                # Pick artwork listings.
                try:
                    products = soup.find_all('figure')
                    # print(len(products))
                    for product in products:
                        link = product.find('a').get('href')
                        if link is not None:
                            if self.website.domain not in link:
                                link = self.link_maker(link)
                            self.artwork_listings.append(link)
                except AttributeError:
                    pass
                try:
                    next_select = soup.find('a', title='Next')
                    next = next_select.get('href')

                    if next is not None:
                        if self.website.domain not in next:
                            next = self.link_maker(next)
                        recurr(next)
                except AttributeError:
                    pass
        recurr(self.website.start_url)

    def get_artwork_data_slave(self, url):
        soup = TheMiner.fetch_page(url)

        if soup is not None:
            artwork = None
            artist = None
            year = None
            price = None
            medium = None
            type_ = None
            dimensions = None
            support = None
            frame = None
            signature = None
            authenticity = None
            about = None
            image_loc = None
            seller_id = None
            artist_id = None
            technique = None
            # Artist_url and artist_id
            try:
                artist_block = soup.find_all('a')
                for a in artist_block:
                    if a.get('data-type') == 'artist-name':
                        artist_url = a['href']

                if self.website.domain not in artist_url:
                    artist_url = self.link_maker(artist_url)
                artist_id = self.get_artist_id(artist_url)
                print(artist_url)
            except AttributeError:
                pass
            except TypeError:
                pass

            # Seller_url and seller_id
            try:
                seller_url = artist_url
                seller_id = self.get_seller_id(seller_url)
            except AttributeError:
                pass

            # medium
            if "painting" in self.website.start_url:
                medium = "Painting"
            elif "sculpture" in self.website.start_url:
                medium = "Sculpture"
            else:
                pass

            # print(medium)

            if artist_id is not None and seller_id is not None and medium is not None:
                # price
                try:
                    price_block = soup.find('div', class_='mix161-0 uyv957-7 jbeESP fcgGpa')
                    if price_block.get('data-status') == 'forSale':
                        price = price_block.find('div', class_='krw7aj-0 uyv957-3 hDfspS brKcXP').text.strip()
                        t = ""
                        for p in price:
                            if p.isnumeric() or str(p) == ".":
                                t += p
                        price = float(t)
                    # price = price_block['data-price'].strip()
                    # price = float(price)*rate
                    # print(price)
                except AttributeError:
                    pass
                except TypeError:
                    pass
                # print(price)

                if price is not None:
                    # artwork and year
                    try:
                        artwork = soup.find('h1')
                        artwork = artwork.text.strip()
                        # print(artwork)
                    except AttributeError:
                        pass

                    # Artist
                    try:
                        artist_block = soup.find_all('a')
                        for a in artist_block:
                            if a.get('data-type') == 'artist-name':
                                artist = a.text.strip()
                                print(artist)
                    except AttributeError:
                        pass

                    # about, year, technique
                    try:
                        about_artwork = soup.find_all('div', class_='yx94r6-2 bTZzNi')
                        for a in about_artwork:
                            if a.get('data-type') == 'about-artwork':
                                details = a.find_all('p')
                                # print(len(details))
                                for p in details:
                                    if p.get('data-type') == 'description':
                                        about = p.text.strip()
                                        # print(about)
                                        continue
                                    if 'Original Created:' in str(p.text):
                                        year = p.span.text
                                        t = ""
                                        for y in year:
                                            if str(y).isnumeric():
                                                t += y
                                        year = int(t)

                                        # print(year)
                                        continue
                                    if 'Materials:' in p.text:
                                        technique = p.parent.text.strip()
                                        technique = re.sub('Materials:', "", technique)
                                        # print(technique)
                    except AttributeError:
                        # print("B")
                        pass
                    except TypeError:
                        pass

                    # type_
                    try:
                        product_details = soup.find_all('div', class_='yx94r6-2 bTZzNi')
                        for detail in product_details:
                            if detail.get('data-type') == 'product-details':
                                deta = detail.find_all('p')
                                print(len(deta))
                                for p in deta:
                                    if re.search(r'Size:\.*', p.text):
                                        dimensions = re.sub('Size:', "", p.text.strip())
                                        print(dimensions)
                                        continue

                                    if 'Frame:' in str(p.text):
                                        frame = str(p.text).strip()
                                        print(frame)
                                        continue

                                    if re.search(r'Original:\.*', p.text):
                                        type_ = str(p.text).strip()
                                        type_ = re.sub("Original:", "", type_)
                                        print(type_)
                                        continue

                    except AttributeError:
                        pass

                    # image_loc
                    try:
                        ima = soup.find('div', class_=re.compile(r'sc-1p3nr8g-12\.*'))
                        image_loc = ima.img['src']
                        print(image_loc)
                    except AttributeError:
                        pass

            #         # signature
            #         try:
            #             signature = soup.find('div', class_='card-body').find('div', class_='mt-4').find('div', class_='text-muted line-height').find('span', class_='text-muted small').text.strip()
            #             print(signature)
            #         except AttributeError:
            #             pass
            #
            #         # authenticity
            #         try:
            #             authenticity = soup.find('div', class_='card-body').find('i', class_='fas fa-certificate font-weight-light mr-1').parent.text.strip()
            #             print(authenticity)
            #         except AttributeError:
            #             pass
            #
                    artwork_bundle = {"artwork_title": artwork, "artist_name": artist, "year": year, "price": price,
                                      "Medium": medium, "Type": type_, "Dimensions": dimensions, "Support": support,
                                      "Frame": frame, "Signature": signature, "Authenticity": authenticity,
                                      "About": about, "platform": self.website.platform, "image_addr": image_loc,
                                      "seller_id": seller_id, "artist_id": artist_id, "url": url,
                                      "technique": technique}
                    # print(artwork_bundle)
                    TheAuthour.write_artwork_price_image(**artwork_bundle)

    def get_artwork_data_master(self):

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.get_artwork_data_slave, self.artwork_listings)
        for result in results:
            pass

    def miner(self):

        # From initial listings we get artwork listings
        self.get_artwork_listings()
        print(len(self.artwork_listings))

        self.get_artwork_data_master()

        # DATA COLLECTION COMPLETED FOR THIS MODULE.
        # DOWNLOADING IMAGES NOW.
        TheMiner.sir_image_manager()

def main():
    # Creating SELLER_INFO === To be used with artwork entry
    sellers = db.Sellers()
    sellers.read_data_sellers()

    # Creating ARTIST_INFO === To be used with artwork entry
    artists = db.Artist()
    artists.read_artist_data()

    # url = 'https://www.saatchiart.com/sculpture/pop-art/fiberglass?hitsPerPage=100&materials%5B%5D=bronze&materials%5B%5D=iron&materials%5B%5D=marble&materials%5B%5D=aluminium&materials%5B%5D=stainless-steel&materials%5B%5D=other&mediums%5B%5D=glass&mediums%5B%5D=bronze&page=90&sort=newest&styles%5B%5D=fine-art&styles%5B%5D=modern'

    webagent = Website('https://www.saatchiart.com',
                               'https://www.saatchiart.com/sculpture/pop-art/fiberglass?hitsPerPage=100&materials%5B%5D=bronze&materials%5B%5D=iron&materials%5B%5D=marble&materials%5B%5D=aluminium&materials%5B%5D=stainless-steel&materials%5B%5D=other&mediums%5B%5D=glass&mediums%5B%5D=bronze&sort=newest&styles%5B%5D=fine-art&styles%5B%5D=modern',
                               "SAATCHIART")
    # webagent = Website('https://www.saatchiart.com',
    #                    'https://www.saatchiart.com/sculpture/pop-art/fiberglass?hitsPerPage=100&materials%5B%5D=bronze&materials%5B%5D=iron&materials%5B%5D=marble&materials%5B%5D=aluminium&materials%5B%5D=stainless-steel&materials%5B%5D=other&mediums%5B%5D=glass&mediums%5B%5D=bronze&page=90&sort=newest&styles%5B%5D=fine-art&styles%5B%5D=modern',
    #                    'SAATCHIART')
    saatchiart_sculpture = Saatchiart(webagent)
    saatchiart_sculpture.miner()

    print("FINISHED")

if __name__ == "__main__":
    main()
