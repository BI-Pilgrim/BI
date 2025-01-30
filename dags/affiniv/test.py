from affiniv.scraper import AffinivScraper

scraper = AffinivScraper(username, password, token=token)
company_id = scraper.login_data.company.id
scraper.get_survey_list(company_id)

import requests, json
payload = json.dumps({"username": username, "password": password})
resp = requests.post("https://app.affiniv.com/api/pub/v1/token", data=payload)



response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
