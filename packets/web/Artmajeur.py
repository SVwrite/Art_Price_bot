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


class Artmajeur:
    def __init__(self, website):
        self.website = website
        self.initial_listings = []
        self.artwork_listings = []

    def link_maker(self, rel_url):
        url = self.website.domain + rel_url
        return url


    #___________LISTINGS_______________________

    def get_initial_listings(self):

        def recurr(url_):
            soup = TheMiner.fetch_page(url_)
            visited.discard(self.website.start_url)
            if soup is not None:
                container = soup.find_all('nav')
                # print(len(container))
                container = container[-2].find_all('li', class_='page-item')
                # print(len(container))
                # print("A")
                for link in container:
                    try:
                        url = link.a['href']
                        if self.website.domain not in url:
                            url = self.link_maker(url)
                        # print(url)
                        if url not in self.initial_listings:
                            self.initial_listings.append(url)
                        # print(len(self.initial_listings))
                    except AttributeError:
                        pass
                    except TypeError:
                        pass
            else:
                pass
                # print("FAILED: SOUP NOT RECEIVED")

        recurr(self.website.start_url)
        i = 0
        while len(self.initial_listings) < 100:
            i += 1
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(recurr, self.initial_listings)

            for result in results:
                pass

            if i > 150:
                break


        visited.clear()
        print(visited)


    #_______________ARTIST_____________________________________

    def get_artist_id(self, artist_url):
        # We go to artist page to pick data we need to make the ARTIST_INFO key.

        artist_id = None
        if artist_url in KEY_INFO.keys():
            key = KEY_INFO[artist_url]
            artist_id = ARTIST_INFO.get(key)
        else:
            try:
                self.get_artist_data(artist_url)
                if artist_url in KEY_INFO.keys():
                    key = KEY_INFO[artist_url]
                    artist_id = ARTIST_INFO.get(key)
                else:
                    # If it ever comes to here, the page will not have an Artist
                    print("FATAL ERROR :: Artist_id not found. Artist_url broken or artist page may be down.")
                # Let's return None here, and not pick rest of the data if the artist_id is not found.
                # Artist id is used in artworks table only.
            except KeyError:
                pass

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
                name = soup.find('h1', class_='h1').text.strip()
                print(name)
            except AttributeError:
                pass
            except TypeError:
                pass

            if name is not None:
                # country
                try:
                    country = soup.find('div', class_='location').text.strip().split(",")
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
    def get_artwork_listings_slave(self, url):
        soup = TheMiner.fetch_page(url)
        if soup is not None:
            section = soup.find('div', id='artwork_list').find('section').find_all('div', class_='col-lg-4 col-md-4 col-sm-12')
            for sec in section:
                products = sec.find_all('article')
                for product in products:
                    try:
                        link = product.find('figure').a['href']
                        if self.website.domain not in link:
                            link = self.link_maker(link)
                        if link not in self.artwork_listings:
                            self.artwork_listings.append(link)
                    except AttributeError:
                        pass
                # print(len(products))
            # print(len(section))

    def get_artwork_listings_master(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.get_artwork_listings_slave, self.initial_listings)
        for result in results:
            pass

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
                artist_block = soup.find('main').find('h2')
                artist_url = artist_block.a['href']
                if self.website.domain not in artist_url:
                    artist_url = self.link_maker(artist_url)
                artist_id = self.get_artist_id(artist_url)
                # print(artist_url)
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

            if artist_id is not None and seller_id is not None and medium is not None:
                # price
                try:
                    price_block = soup.find('span', id='analytics_data_price')
                    price = price_block['data-price'].strip()
                    price = float(price)*rate
                    # print(price)
                except AttributeError:
                    pass
                except TypeError:
                    pass

                if price is not None:
                    # artwork and year
                    try:
                        artwork = soup.find('h1').find('span').text.strip()
                        t = ""
                        for y in artwork:
                            if str(y) == '\n':
                                break
                            if str(y) != "":
                                t += y
                        artwork = t
                        print(artwork)

                        year = soup.find('h1').find('span', class_='text-muted').text.strip()
                        t = ""
                        for y in year:
                            if str(y).isdigit():
                                t += y
                        year = int(t)
                        if year > 3000:
                            year = int(str(year)[0:4])
                        print(year)
                    except AttributeError:
                        pass

                    # artist's name
                    try:
                        artist = soup.find('main').find('h2').find('a').text.strip()
                    except AttributeError:
                        pass

                    # type, technique, support, frame,
                    try:
                        data = soup.find('div', class_='border-top mt-4 pt-4')
                        data = data.find_all('div', class_='row mt-4')
                        # print(len(data))
                        # print(data)
                        for d in data:
                            # print("A")
                            # print(d.div.text.strip())
                            if 'Dimensions' in str(d.div.text).strip():
                                dimensions = d.find('div', class_='d-flex align-items-start col-6 col-sm-9').text.strip()
                                # print(dimensions)
                            elif 'Techniques' in str(d.div.text):
                                technique = d.a.text.strip()
                                # print(technique)
                            elif 'Support or surface' in str(d.div.text):
                                support = d.find('div', class_='d-flex align-items-start col-6 col-sm-9').text.strip()
                                # print(support)
                            elif 'Framing' in str(d.div.text):
                                frame = d.find('div', class_='d-flex align-items-start col-6 col-sm-9').text.strip().split("  ")
                                t = ""
                                for i in frame:
                                    if i != "" and str(i) != '\n' and str(i) != '\t':
                                        t += i
                                        t += " "
                                frame = t
                                print(frame)
                        # print(data)
                    except AttributeError:
                        # print("B")
                        pass

                    # type
                    try:
                        types = soup.find('div', class_='border-top mt-4 pt-4').find('div', class_='row')
                        if 'Artwork Type' in str(types.div.text).strip():
                            type_ = types.find('button', class_='btn btn-link pl-0').text.strip()
                            # print(type_)
                    except AttributeError:
                        pass

                    # image_loc
                    try:
                        image_loc = soup.find('div', id='carousel_image').find('img', class_='img-main').get('src')
                        # print(image_loc)
                    except AttributeError:
                        pass
                    except TypeError:
                        pass

                    # signature
                    try:
                        signature = soup.find('div', class_='card-body').find('div', class_='mt-4').find('div', class_='text-muted line-height').find('span', class_='text-muted small').text.strip()
                        # print(signature)
                    except AttributeError:
                        pass

                    # authenticity
                    try:
                        authenticity = soup.find('div', class_='card-body').find('i', class_='fas fa-certificate font-weight-light mr-1').parent.text.strip()
                        # print(authenticity)
                    except AttributeError:
                        pass

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
        self.get_initial_listings()
        print(len(self.initial_listings))


        # From initial listings we get artwork listings
        self.get_artwork_listings_master()

        self.get_artwork_data_master()


        # DATA COLLECTION COMPLETED FOR THIS MODULE.
        # DOWNLOADING IMAGES NOW.
        TheMiner.sir_image_manager()

        #TEST ZONE
        # url = 'https://www.artmajeur.com/en/odi-faure/artworks/14646098/pivoine-05'
        # self.get_artwork_data_slave(url)

        # url = 'https://www.artmajeur.com/tatiana-rezvaya'
        # self.get_artist_data(url)



def main():
    # Creating SELLER_INFO === To be used with artwork entry
    sellers = db.Sellers()
    sellers.read_data_sellers()

    # Creating ARTIST_INFO === To be used with artwork entry
    artists = db.Artist()
    artists.read_artist_data()

    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/painting/oil-390',
                               "ARTMAJEUR")
    artmajeur_paintingOil = Artmajeur(webagent)
    artmajeur_paintingOil.miner()

    time.sleep(10)

    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/painting/watercolor-399',
                               "ARTMAJEUR")
    artmajeur_paintingWatercolour = Artmajeur(webagent)
    artmajeur_paintingWatercolour.miner()

    time.sleep(10)

    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/sculpture/bronze-1432',
                               "ARTMAJEUR")
    artmajeur_sculptureBronze = Artmajeur(webagent)
    artmajeur_sculptureBronze.miner()

    time.sleep(10)

    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/sculpture/glass-3',
                               "ARTMAJEUR")
    artmajeur_sculptureGlass = Artmajeur(webagent)
    artmajeur_sculptureGlass.miner()


    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/sculpture/terra-cotta-113',
                               "ARTMAJEUR")
    artmajeur_sculptureTerracotta = Artmajeur(webagent)
    artmajeur_sculptureTerracotta.miner()


    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/painting/pop-art-615',
                               "ARTMAJEUR")
    artmajeur_paintingPopArt = Artmajeur(webagent)
    artmajeur_paintingPopArt.miner()


    webagent = Website('https://www.artmajeur.com',
                               'https://www.artmajeur.com/en/artworks/sculpture/wood-5',
                               "ARTMAJEUR")
    artmajeur_sculptureWood = Artmajeur(webagent)
    artmajeur_sculptureWood.miner()


    time.sleep(10)

    print("FINISHED")

if __name__ == "__main__":
    main()
