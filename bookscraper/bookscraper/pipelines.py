from itemadapter import ItemAdapter
import psycopg2

class BookscraperPipeline:
    """Cleans and normalizes the scraped book data."""
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip whitespace
        for field_name in adapter.field_names():
            value = adapter.get(field_name)
            if isinstance(value, str):
                adapter[field_name] = value.strip()

        # Lowercase certain fields
        for key in ['category', 'product_type']:
            value = adapter.get(key)
            if value:
                adapter[key] = value.lower()

        # Convert price strings (£12.99) to float
        for key in ['price', 'price_excl_tax', 'price_incl_tax', 'tax']:
            value = adapter.get(key)
            if value:
                value = value.replace('£', '')
                adapter[key] = float(value)

        # Extract number from availability text
        avail = adapter.get('availability', '')
        if '(' in avail:
            adapter['availability'] = int(avail.split('(')[1].split(' ')[0])
        else:
            adapter['availability'] = 0

        # Convert string numbers
        adapter['num_reviews'] = int(adapter.get('num_reviews', 0))

        # Convert star rating text to number
        stars_text = adapter.get('stars', '').lower()
        words = stars_text.split()
        if len(words) > 1:
            mapping = {"zero":0, "one":1, "two":2, "three":3, "four":4, "five":5}
            adapter['stars'] = mapping.get(words[1], 0)

        return item


class SaveToPostgresPipeline:
    """Saves cleaned data into PostgreSQL."""
    def __init__(self):
        hostname = 'localhost'
        username = 'scrapy_user'
        password = 'scrapy_password'
        database = 'scrapy_db'

        # Connect to database
        self.connection = psycopg2.connect(
            host=hostname,
            user=username,
            password=password,
            dbname=database
        )
        self.cur = self.connection.cursor()

        # Create table if not exists
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS books(
                id serial PRIMARY KEY,
                url VARCHAR(255),
                title TEXT,
                upc VARCHAR(255),
                product_type VARCHAR(255),
                price_excl_tax DECIMAL,
                price_incl_tax DECIMAL,
                tax DECIMAL,
                price DECIMAL,
                availability INTEGER,
                num_reviews INTEGER,
                stars INTEGER,
                category VARCHAR(255),
                description TEXT
            )
        """)

    def process_item(self, item, spider):
        # Insert scraped data
        self.cur.execute("""
            INSERT INTO books (
                url, title, upc, product_type,
                price_excl_tax, price_incl_tax, tax, price,
                availability, num_reviews, stars, category, description
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            item.get('url'),
            item.get('title'),
            item.get('upc'),
            item.get('product_type'),
            item.get('price_excl_tax'),
            item.get('price_incl_tax'),
            item.get('tax'),
            item.get('price'),
            item.get('availability'),
            item.get('num_reviews'),
            item.get('stars'),
            item.get('category'),
            str(item.get('description'))
        ))

        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()
