from itemadapter import ItemAdapter

class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all the white spaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            value = adapter.get(field_name)
            if isinstance(value, list) and value:  # Handle list types and ensure it's not empty
                adapter[field_name] = value[0].strip()
            elif isinstance(value, str):  # Handle string types directly
                adapter[field_name] = value.strip()

        # Category and product type switch to lower case
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            if value:  # Ensure value is not None
                adapter[lowercase_key] = value.lower()

        # Price convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            if value:  # Ensure value is not None
                # Remove any currency symbols (£, $, etc.)
                value = value.replace('£', '').replace('$', '').strip()
                try:
                    adapter[price_key] = float(value)
                except ValueError:
                     
                     spider.logger.warning(f"Could not convert {price_key} value '{value}' to float.")
                     adapter[price_key] = None  # or handle appropriately

        # Availability exact number of books in store
        availability_string = adapter.get('availability')
        if availability_string:
            split_string_array = availability_string.split('(')
            if len(split_string_array) < 2:
                adapter['availability'] = 0
            else:
                availability_array = split_string_array[1].split()
                adapter['availability'] = int(availability_array[0])

        # Reviews convert to int
        reviews_string = adapter.get('num_of_reviews')
        if reviews_string:
            adapter['num_of_reviews'] = int(reviews_string)

        # Stars convert into int
        stars_string = adapter.get('stars')
        if stars_string:
            split_stars_array = stars_string.split(' ')
            if len(split_stars_array) > 1:  # Ensure there are at least two elements
                stars_text_value = split_stars_array[1].lower()
                stars_mapping = {
                    'zero': 0,
                    'one': 1,
                    'two': 2,
                    'three': 3,
                    'four': 4,
                    'five': 5
                }
                adapter['stars'] = stars_mapping.get(stars_text_value, 0)
            else:
                spider.logger.warning(f"Unexpected format for stars: '{stars_string}'")
                adapter['stars'] = 0  # Default value if the format is unexpected
        else:
            adapter['stars'] = 0  # Handle missing or empty stars field

        return item