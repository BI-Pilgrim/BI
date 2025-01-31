from affiniv.scraper import AffinivScraper

from airflow.models import Variable

username = Variable.get("affiniv_username")
password = Variable.get("affiniv_password")

scraper = AffinivScraper(username, password)
company_id = scraper.login_data.company.id
scraper.get_survey_list(company_id)

