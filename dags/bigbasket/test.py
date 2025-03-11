import json
import requests


session = requests.Session()
default_headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
  'Accept': '*/*',
  'Accept-Language': 'en-US,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate, br, zstd',
  'content-type': 'application/json; charset=utf-8',
  'X-Tenant-ID': '1',
  'Origin': 'https://nucleus.bigbasket.com',
  'Connection': 'keep-alive',
  'Referer': 'https://nucleus.bigbasket.com/',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
  'DNT': '1',
  'Sec-GPC': '1',
  'Priority': 'u=0',
  'Pragma': 'no-cache'
}
session.headers.update(default_headers)

resp = session.post("https://nucleus.bigbasket.com/authz/v1/vendor/otp/", data=json.dumps({"email":"mini@discoverpilgrim.com"}))

# Get OTP from email with query subject:Vendor Dashboard OTP AND is:unread

otp_payload = json.dumps({"email":"mini@discoverpilgrim.com","otp":474137})
resp = session.post("https://nucleus.bigbasket.com/authz/v1/vendor/login/", data=otp_payload)
session.get("https://nucleus.bigbasket.com/stitch/vendor/reports/")