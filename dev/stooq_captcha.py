import requests as rq
from bs4 import BeautifulSoup as bs
from playwright.sync_api import sync_playwright
import base64


from workers import api as api
from workers import common

STOOQ_HEADER = "./assets/header_stooq.jsonc"

url = 'https://stooq.com/q/d?s=2388.hk&d1=20230331&d2=20230717&l=1'

header, cookies = common.set_header(STOOQ_HEADER)
resp = rq.get(url=url, headers=header, cookies=cookies)
page = bs(resp.content, 'lxml')


def GDPR():
    # GDPR dialog
    # when no cookies present, first time use
    # will set proper headers and cookies
    print('Setting https connection...')
    def header(req):
        req_header.update(req.headers)
    
    req_header = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url=url)
        page.on('request', lambda req: header(req))
        page.locator('button.fc-cta-consent').click()
        req_header['cookies'] = context.cookies()

    common.set_header(file=STOOQ_HEADER, upd_header=req_header)

if page.body is None:
    GDPR()
    header, cookies = common.set_header(STOOQ_HEADER)
    resp = rq.get(url=url, headers=header, cookies=cookies)
    page = bs(resp.content, 'lxml')


