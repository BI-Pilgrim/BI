from judgeme.scraper import JudgeMeScraper

scraper = JudgeMeScraper(api_key=API_KEY, shop_domain=SHOP_DOMAIN)
scraper.get_reviews()