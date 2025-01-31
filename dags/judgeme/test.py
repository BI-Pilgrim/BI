from judgeme.scraper import JudgeMeScraper

from airflow.models import Variable
from utils.google_cloud import get_bq_client

API_KEY = Variable.get("judgeme_api_key")
SHOP_DOMAIN = Variable.get("judgeme_shop_domain")


credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
bq_client = get_bq_client(credentials_info)

query = """
            SELECT MAX(id) as max_id
            FROM `pilgrim_bi_judgeme.reviews`
        """

q = bq_client.query(query)
q.result()

scraper = JudgeMeScraper(api_key=API_KEY, shop_domain=SHOP_DOMAIN)
scraper.get_reviews()