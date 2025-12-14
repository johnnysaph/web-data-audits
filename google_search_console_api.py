from google_api_service import get_service
import pandas as pd

SCOPES = [
    'https://www.googleapis.com/auth/webmasters.readonly',
]
    
class GoogleSearchConsoleClient():
    """Class which incapsulates connection to Google Search Console API
	https://googleapis.github.io/google-api-python-client/docs/dyn/searchconsole_v1.html
    """    
    def __init__(self):
        self._service = get_service('searchconsole', 'v1', SCOPES)
        self.analytics = self._service.searchanalytics()
            
    def get_daily_report(self, site_url, dimensions, date, row_limit=25000):
        body = {
            "aggregationType": "auto",
            "dimensions": dimensions,
            "startDate": date,
            "endDate": date,
            "rowLimit": row_limit
        }
        try:
            r = self.analytics.query(siteUrl=site_url, body=body).execute()
        except Exception as ex:
            print('The following error occurred: {}'.format(ex))
            return pd.DataFrame()
        else:
            if 'rows' in r: # got the data from sc
                df = pd.DataFrame.from_records(r['rows'])
                df['date'] = date
                df['website'] = site_url
                for i, dimension in enumerate(dimensions):
                    df[dimension] = df['keys'].apply(lambda x: x[i])
                del df['keys']
                return df
            else: # no data in sc for the current date
                print('Empty response from API')
                return pd.DataFrame()