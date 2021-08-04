import time

import pymysql



class Query:
    def __init__(self):
        i = 0
        while True:
            i += 1
            try:
                self.mydb, self.my_cursor = self.connection()
                break
            except pymysql.err.OperationalError:
                print(f"Connection to database failed. Trying again in {i} seconds.")
                time.sleep(i)

    def __del__(self):
        print("Disconnecting from database...")
        self.mydb.close()

    def connection(self):
        # def connect():
        # try:
        mydb = pymysql.connect(
            host="localhost",
            user="root",
            passwd="4074",
        )

        my_cursor = mydb.cursor()

        # my_cursor.execute("CREATE DATABASE IF NOT EXISTS Artwork_Comparator")
        try:
            my_cursor.execute("USE Artwork_Comparator")
            print(f"Connected to database")
            return mydb, my_cursor
        except pymysql.err.InternalError:
            print("Database 'Artwork_comparator' not found.")
            pass

    def read_artworks_title(self, artwork):
        query = """SELECT * FROM artworks WHERE ARTWORK_TITLE LIKE %s"""
        self.my_cursor.execute(query, [str(artwork)])

        results = list(self.my_cursor.fetchall())
        artwork_ids = []
        artist_ids = []
        for result in results:
            # artwork_id is at 14th index
            artwork_ids.append(result[14])
            artist_ids.append(result[13])

        if len(artwork_ids) > 0:
            for i in range(len(artwork_ids)):
                print()
                print('ARTWORK DETAILS')
                print(results[i])
                print( 'ARTIST DETAILS ')
                print()

                a_id = artist_ids[i]
                self.read_artists_id(a_id)
                print(' PRICE DETAILS ')
                print()

                art_id = artwork_ids[i]
                self.read_prices_artwork_id(art_id)
                print()


    def read_artists_name(self, artist):
        pass
        # SELECT ARTIST_ID from artists WHERE name LIKE '%{artist}%';

    def read_artists_id(self, artist_id):
        query = """SELECT * FROM artists WHERE ARTIST_ID = %s"""
        self.my_cursor.execute(query, [artist_id])
        results = self.my_cursor.fetchall()
        if len(results) > 0:
            print(results)
        else:
            print(" No artist found for this product")

    def read_seller(self, seller):
        pass

    def read_seller_id(self, seller_id):
        query = """SELECT * FROM sellers WHERE SELLER_ID = %s"""
        self.my_cursor.execute(query, [seller_id])
        results = self.my_cursor.fetchall()
        if len(results) > 0:
            print(results)
        else:
            print(" No artist found for this product")

    def read_prices_artwork_id(self, artwork_id):
        query = """SELECT * FROM prices WHERE ARTWORK_ID = %s"""
        self.my_cursor.execute(query, [artwork_id])
        results = self.my_cursor.fetchall()

        if len(results) > 0:
            # print(results)

            for result in results:
                print()

                seller_id = result[2]
                print(result)
                print()

                print(' SELLER DETAILS')
                self.read_seller_id(seller_id)
                print()


        else:
            print(" No price entries found for this product")


def main():
    artwork_title = input("PLEASE ENTER ARTWORK'S NAME :")
    reader = Query()
    reader.read_artworks_title(artwork_title)

if __name__ == "__main__":
    main()