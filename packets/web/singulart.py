import concurrent.futures
# from bs4 import BeautifulSoup
import time
import re

# Importing data structures
from packets.websiteds import Website
from packets.dataStructures import TheAuthour

from packets.TheMiner import TheMiner
from packets import dbmanip as db
from packets.global_vars import SELLER_INFO, ARTIST_INFO, visited, KEY_INFO


class Singulart:
    # NOTES ABOUT THE SITE
    def __init__(self, website):
        self.website = website
        self.artist_listings = []
        self.artwork_listings = []
        self.listy = []

    def link_maker(self, rel_url):
        url = self.website.domain + rel_url
        return url

    def key_maker(self, artist_url):
        # Fetches the artist's data and makes a key out of it, and returns it??? Why?? Call get_artist_data. It'll
        # make the key and everything.
        visited.discard(artist_url)
        soup = TheMiner.fetch_page(artist_url, ghost=True)
        if soup is not None:
            self.get_artist_data(soup, artist_url)
        else:
            pass

    # ___________ ARTISTS____________________
    # Called by miner (1).
    def get_artist_listings(self):

        def recurr(url):
            soup = TheMiner.fetch_page(url, ghost=True)
            if soup is not None:
                # Because singulart keeps blocking ips, we'll ship everything inside try-except statements.
                try:
                    # artist_blocks = soup.find_all('div', class_='artist-container')
                    artist_blocks = soup.find_all('figure', class_='pic-artist')
                    print(len(artist_blocks))
                    for artist in artist_blocks:
                        link = artist.figcaption.h2.a.get('href')
                        if self.website.domain not in link:
                            link = self.link_maker(link)
                        self.artist_listings.append(link)
                    # print(self_artist_listings)

                    # next pages
                    next_pages = soup.find('div', class_='pagerfanta').find('nav')
                    next_pages = next_pages.find_all('a')
                    for next_ in next_pages:
                        link = next_.get('href')
                        if self.website.domain not in link:
                            link = self.link_maker(link)
                        if link not in self.listy:
                            self.listy.append(link)

                    # print(listy)
                    # print(len(listy))

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        trig = executor.map(recurr, self.listy)
                    # trig = map(recurr, self.listy)
                    for trigger in trig:
                        pass
                except AttributeError:
                    print("REQUEST FAILED: IP blocked by the website.")
                    visited.discard(url)
                    pass

        while len(self.listy) == 0 or len(self.artist_listings) == 0:
            recurr(self.website.start_url)

        while len(self.listy) <= 70 or len(self.artist_listings) <= 3000:
            result = map(recurr, self.listy)
            for r in result:
                pass

    def get_artist_data(self, soup, url):
        # name, born, country, about
        # pack = [name, born, country, about]
        # no need to run the safety try: except: here because we're not fetching the page here.
        try:
            name = soup.find('div', class_='artist-intro').find('h1').text
            name = str(name).strip()
        except AttributeError:
            name = None

        if name is not None:
            try:
                born = soup.find('p', class_='born').text.strip()
                t = ""
                for b in born:
                    if str(b).isdigit():
                        t += b
                born = int(t)

                if born > 3000:
                    born = str(born)[0:3]

            except AttributeError:
                born = None
            except ValueError:
                born = None

            # Country
            try:
                country = soup.find('div', class_="artist-intro")
                country = country.find('div', class_='h2').text.strip().split("|")
                country = str(country[-1]).strip()
            except AttributeError:
                country = None

            # About
            try:
                about = soup.find('section', class_='artist-bio')
                about = about.find('div', class_='resume').text.strip()
            except AttributeError:
                about = None

            # pack = [name, born, country, about]
            # print(pack)

            artist_data_pack = [name, born, country, about]
            # pack = [name, born, country, about]
            # Updating KEY_INFO dictionary.
            KEY_INFO[url] = db.Artist.key_maker(artist_data_pack)
            # Updating the dB with artist listings.
            TheAuthour.write_artist(*artist_data_pack)

    # Gets artist data as well through slave -> get_artist_data -> write_artist_data
    def get_artist_id(self, artist_url):
        # We go to artist page to pick data we need to make the ARTIST_INFO key.

        artist_id = None
        if artist_url in KEY_INFO.keys():
            key = KEY_INFO[artist_url]
            artist_id = ARTIST_INFO[key]
        else:
            self.key_maker(artist_url)
            if artist_url in KEY_INFO.keys():
                key = KEY_INFO[artist_url]
                artist_id = ARTIST_INFO[key]
            else:
                # If it ever comes to here, the page will not have an Artist
                print("FATAL ERROR :: Artist_id not found. Artist_url broken or artist page may be down.")
            # Let's return None here, and not pick rest of the data if the artist_id is not found.
            # Artist id is used in artworks table only.

        return artist_id
    # ________________________________________
    # _____________ARTWORK LISTINGS___________

    def get_artwork_listings_slave(self, url):

        soup = TheMiner.fetch_page(url, ghost=True)
        # Artist's info and artwork listings are available on the same page.
        if soup is not None:
            try:
                name = soup.find('div', class_='artist-intro').find('div', class_='content').h1.text
                # Name will cause the crash if the page is not returned
                block = soup.find_all('div', class_='artist-container artist-container--details')
                # print(f"BLOCK : {len(block)}")
                try:
                    for chunk in block:
                        items = chunk.find_all('figure', class_='artwork-item artwork-item--details')
                        # print(f"ITEMS : {len(items)}")

                        for piece in items:
                            paise = piece.find('div', class_='meta').text.strip()
                            # print(paise)
                            if "Sold" not in str(paise):
                                # print("B")
                                a = piece.find('a')['href']
                                if self.website.domain not in a:
                                    a = self.link_maker(a)
                                if a not in self.artwork_listings:
                                    self.artwork_listings.append(a)

                except AttributeError:
                    # print("A")
                    pass

                self.get_artist_data(soup, url)

            except AttributeError:
                print("IP blocked by website. Request failed.")
                # Urls that get blocked are discarded from visited and added to listy for a recall. (linear if listy is
                # small and multithreaded if listy is large enough till, its brought of size.
                visited.discard(url)
                self.listy.append(url)

    def get_artwork_listings_master(self):
        # Takes the list artwork listings and form it, for each url,
        # 1. Picks artist's data
        # 2. Picks artwork(product) listings
        self.listy.clear()
        # Clearing listy for use later.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.get_artwork_listings_slave, self.artist_listings)
        for result in results:
            pass

        # As long as there is any element in listy. We keep calling the slave. If listy > 20 we thread.
        # if listy < 20, we process it linearly
        i = 0
        while len(self.listy) > 0:
            i += 1
            if len(self.listy) > 20:
                print(f"ROUND {i}, {len(self.listy)} entries remain.")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = executor.map(self.get_artwork_listings_slave, self.listy)
                self.listy.clear()
                for r in results:
                    pass
            else:
                print(f"ROUND {i}, {len(self.listy)} entries remain.")
                results = map(self.get_artwork_listings_slave, self.listy)
                self.listy.clear()
                for r in results:
                    pass

    # ________________________________________
    # _____________SELLERS____________________

    def get_seller_id(self, seller_url) -> int:
        # Fetches seller_data, writes it in db, and returns seller_id.
        # bundle = [seller_url, self.website.platform, 'KAZoART', None, url]
        seller_id = None

        if seller_url is not None:
            if seller_url in SELLER_INFO.keys():
                seller_id = SELLER_INFO.get(seller_url)
                # print(seller_id)
            else:
                # If code reaches here then the entry for seller doesn't already exists. Let's call get_seller_data
                # again with seller_url
                self.get_seller_data(seller_url)
                # wait for a second to make sure that transaction is smooth. Activate this line if errors are thrown.
                # time.sleep(1)
                # Try to fetch seller data again.
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

    # _____________ARTWORK DATA___________

    # Does major mining operations.
    def get_artwork_data_slave(self, url):
        soup = TheMiner.fetch_page(url, ghost=True)
        if soup is not None:
            # Initiation
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
            try:
                # Fail safe block. If the ip has been blocked, an error will be thrown here. we save the page's url for
                # rerun
                # Artist_url
                artist_url = soup.find('section', class_='artwork-focus')
                # print(artist_url)
                artist_url = artist_url.find_all('div', class_='col-md-12 col-lg-6')
                try:
                    artist_url = artist_url[1].find('h2').a['href']
                    if self.website.domain not in artist_url:
                        artist_url = self.link_maker(artist_url)
                except AttributeError:
                    artist_url = None

                # Artist_id
                artist_id = self.get_artist_id(artist_url)
                seller_url = artist_url
                # Seller_url  = artist_url (KAZoART style)

                # Medium
                if "painting" in str(self.website.start_url):
                    medium = "Painting"
                elif "sculpture" in str(self.website.start_url):
                    medium = "Sculpture"
                else:
                    medium = None
                # print(medium)

                # Price
                try:
                    price_box = soup.find('main', class_='single').find('div', class_='col-md-4 col-lg-3 sidebar')
                    price_box = price_box.find('div', class_='box-price-mobile').find('div', class_='prices')
                    price = price_box.text
                    # Filter
                    t = ""
                    for p in price:
                        if str(p) == "-":
                            break
                        if str(p).isdigit():
                            t += str(p)
                    price = int(t)
                except ValueError:
                    price = None
                except AttributeError:
                    price = None
                except TypeError:
                    price = None

                # print(price)

                if price is not None and medium is not None:

                    # Artwork
                    try:
                        artwork_block = soup.find('section', class_='artwork-focus').find_all('div',
                                                                                              class_='col-md-12 col-lg-6')
                        artwork_block = artwork_block[1].find('h1').text.strip().split(",")
                        artwork = ""
                        for a in range(len(artwork_block) - 1):
                            artwork += artwork_block[a].strip()
                            artwork += " "
                        # print(artwork)
                    except AttributeError:
                        artwork = None

                    # year
                    try:
                        # year
                        artwork_block = soup.find('section', class_='artwork-focus').find_all('div',
                                                                                              class_='col-md-12 col-lg-6')
                        artwork_block = artwork_block[1].find('h1').text.strip().split(",")
                        year = artwork_block[-1]
                        t = ""
                        for y in year:
                            if str(y).isdigit():
                                t += y
                        year = int(t)
                        if year > 3000:
                            year = int(str(year)[0:4])
                        # print(year)
                    except AttributeError:
                        year = None
                    except TypeError:
                        year = None

                    # artist
                    try:
                        artist_block = soup.find('section', class_='artwork-focus').find_all('div',
                                                                                             class_='col-md-12 col-lg-6')
                        artist_block = artist_block[1].find('h2').a.text.strip().split(",")
                        artist = str(artist_block[0]).strip()
                        # print(artist)
                    except AttributeError:
                        artist = None

                    # Technique, frame, dimensions
                    try:
                        artwork_block = soup.find('section', class_='artwork-focus').find_all('div',
                                                                                              class_='col-md-12 col-lg-6')
                        artwork_block = artwork_block[1].find('div', class_='artwork-details').find('ul',
                                                                                                    class_='artwork-details-list')
                        blocks = artwork_block.find_all('li')
                        # print(f"Z : {len(blocks)}")
                    except AttributeError:
                        blocks = None

                    if blocks is not None and type(blocks) is not list:
                        for li in blocks:
                            title = str(li.find('div', class_='title').text).strip()
                            value = str(li.find('div', class_='info').text).strip().split("  ")
                            t = ""
                            for v in value:
                                if str(v) != "" and str(v) != '\n':
                                    t += str(v)
                                    t += " "
                            value = t
                            # print(title, value)
                            # print(re.search('frame', str(value)))

                            if "Technique" in str(title):
                                technique = value
                                continue
                            if "Other details" in str(title) or re.search('frame', str(value)):
                                frame = value
                                continue
                            else:
                                frame = None
                            if "Dimensions" in str(title):
                                dimensions = value
                                continue
                    else:
                        pass

                    # auth\
                    try:
                        blocks = soup.find('section', class_='box-infos').find('div', class_='info-certificate')
                        authenticity = blocks.find('div', class_='certificate-title').text.strip()
                        # print(authenticity)
                    except AttributeError:
                        pass

                    # about
                    try:
                        about = soup.find('section', class_='artwork-details').find('div', id='a-desc').p.text.strip()
                        # filter
                        about = about.split("  ")
                        t = ""
                        for a in about:
                            if a != "" and a != "\n":
                                t += a
                                t += " "
                        about = t
                    except AttributeError:
                        pass

                    # image_loc
                    try:
                        image = soup.find('section', class_='artwork-main pt').find('picture').img['src']
                        # print(f"IMAGE : {image}]")
                        image_loc = image
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

            except AttributeError:
                # Comes here if the page is not returned by the website.
                print("FAILED")
                visited.discard(url)
                self.listy.append(url)
        else:
            print(f"SOUP NOT RETURNED FOR {url}]")

    def get_artwork_data_master(self):
        self.listy.clear()
        # Clearing listy for use later.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.get_artwork_data_slave, self.artwork_listings)
        for result in results:
            pass

        # As long as there is any element in listy. We keep calling the slave. If listy > 20 we thread.
        # if listy < 20, we process it linearly
        while len(self.listy) > 0:
            if len(self.listy) > 20:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = executor.map(self.get_artwork_data_slave, self.listy)
                self.listy.clear()
                for r in results:
                    pass
            else:
                results = map(self.get_artwork_listings_slave, self.listy)
                self.listy.clear()
                for r in results:
                    pass

    def miner(self):
        self.get_artist_listings()
        print(len(self.artist_listings))

        self.get_artwork_listings_master()
        # get_artwork_listings_master -> get_artwork_listings_slave -> get_artist_data -> write_artist_data
        # So we're done with artist data.
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

    webagent = Website('https://www.singulart.com',
                               'https://www.singulart.com/en/painting?count=60',
                               "SINGULART")
    singulart = Singulart(webagent)
    singulart.miner()

    time.sleep(10)

    print("FINISHED")

if __name__ == "__main__":
    main()
