#!/usr/bin/python
# -*- coding: utf-8 -*-

from google_analytics4_api import GA4Client
from urllib.parse import urlparse, urlunparse
from bs4 import BeautifulSoup
from datetime import timedelta
from datetime import datetime
from datetime import date
import pandas as pd
import pandas_gbq
import requests

# ga4 report settings
ga4_property_id = '<YOUR_GA4_PROPERTY_ID>'
dimensions = ["fullPageUrl"]
metrics = ["totalUsers"]

# big query creds
bq_project_id = '<YOUR_BQ_PROPERTY_ID>'
table_id = f'{bq_project_id}.canonicals'

# dates
date_format = '%Y-%m-%d'
day = timedelta(days=1)
week = timedelta(days=8)
start_date = datetime.strftime(date.today() - week, date_format)
finish_date = datetime.strftime(date.today(), date_format)

sql_to_bq = f"""
WITH ranked AS (
  SELECT
    date,
    requested_url,
    status_code,
    actual_url,
    canonical_url,
    ROW_NUMBER() OVER (
      PARTITION BY requested_url
      ORDER BY date DESC
    ) AS rn
  FROM `{table_id}`
)
SELECT
  date,
  requested_url,
  status_code,
  actual_url,
  canonical_url
FROM ranked
WHERE rn = 1;
"""

def clean_url(x):
    p = urlparse(x)
    return urlunparse((p.scheme, p.netloc, p.path, '', '', ''))

def get_canonicals(url, links):
    canonicals = []
    for link in links:
        try:
            rels = link["rel"]
        except KeyError:
            print("Strange thing # 1 detected. Url: {}, link: {}.".format(url, link))
            continue
        if len(rels) > 0 and "canonical" in rels:
            try:
                canonical = link["href"]
                if "{{canonical}}" not in canonical:
                    canonicals.append(canonical)
            except KeyError:
                print("Strange thing # 2 detected. Url: {}, link: {}.".format(url, link))
                continue
    if canonicals:
        return ';'.join(canonicals)
    else:
        return "No Canonical Urls"
   
def process():

    # getting data from bq
    bq_df = pandas_gbq.read_gbq(sql_to_bq, project_id=bq_project_id)
    
    # get URLs from GA4
    ga4 = GA4Client(ga4_property_id)
    df = ga4.run_report(start_date, finish_date, dimensions, metrics)
    #df["clear_url"] = df["fullPageUrl"].apply(lambda x: x.split("?")[0].split("#")[0])
    df["clear_url"] = df["fullPageUrl"].apply(clean_url)
    df.drop_duplicates(subset="clear_url", inplace=True)
    df.sort_values(by='totalUsers', ascending=False, inplace=True)
    df = df[df['totalUsers'] >= 50]
    urls = df["clear_url"]
    print('Ready to process {} urls.'.format(len(urls)))

    # setting http connection   
    s = requests.Session()
    s.headers.update({"User-Agent": "canonical-audit-bot/1.0"})
    
    # parsing
    today = date.today()
    new_records = []
    for url in urls:
                   
        requested_url = url
            
        # get the current data for the url
        r = s.get(requested_url, timeout=10)
        
        # check for redirects
        if requested_url != r.url:
            print("Geo redirect detected. URL: {}.".format(requested_url))
            status_code = 302
            # try geo redirects
            ignore_redirects_url = requested_url + "?ignoreredirects=true"
            r = s.get(ignore_redirects_url, timeout=10)
        else:
            status_code = r.status_code
        
        # get canonicals
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('link')
        canonicals = get_canonicals(requested_url, links)
        record = (today, requested_url, status_code, r.url, canonicals)
        
        # get the previous data for the url and compare it with the current state
        last_added_row = bq_df[bq_df['requested_url'] == url]
        if not last_added_row.empty:
            last_record = last_added_row.iloc[0]
        else:
            last_record = None 
        if last_record and (
            last_record["status_code"] == status_code and
            last_record["actual_url"] == r.url and
            last_record["canonical_url"] == canonicals
        ):
            continue
        new_records.append(record)

        
    # uploading to bq
    final_df = pd.DataFrame(data=new_records, columns=["date", "requested_url", "status_code", "actual_url", "canonical_url"])
    print('Ready to upload {} new records.'.format(len(final_df)))
    pandas_gbq.to_gbq(
    final_df, table_id, project_id=bq_project_id, if_exists='append',
    )
           
if __name__ == "__main__":
    process()