from google_api_service import get_service
import pandas as pd


SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly'
]
    
class GA4Client():
    """Class which incapsulates connection to Google Analytics 4 API
    All the methods could be found by link:
    https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsdata_v1beta.html
	All the dimensions and metrics could be found by link:
	https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
    """

    def __init__(self, property_id):
        self._service = get_service("analyticsdata", "v1beta", SCOPES)
        self.property_id = property_id
        self.property_str = f'properties/{property_id}'

    @staticmethod
    # return empty df if there is no data values
    def _report_to_dataframe(rep):
        header = [val['name'] for val in rep['dimensionHeaders']]
        header.extend([val['name'] for val in rep['metricHeaders']])

        rows = []
        # checking if rows (values) are in rep
        if 'rows' in rep:
            for r in rep['rows']:
                row = [v['value'] for v in r['dimensionValues']]
                row.extend([v['value'] for v in r['metricValues']])
                rows.append(row)

            df = pd.DataFrame(rows, columns=header)
            return df 
        if 'rows' not in rep:
            return pd.DataFrame()
        
    def run_report(self, start_date, end_date, dimensions_list, metrics_list):
        body = {
            'dateRanges': [{
                'startDate': start_date,
                'endDate': end_date
            }],
            'dimensions': [
                {'name': name} for name in dimensions_list
            ],
            'metrics': [
                {'name': name} for name in metrics_list
            ],
            'property': self.property_str,
            'returnPropertyQuota': True
        }

        response = self._service.properties().runReport(property=self.property_str, body=body, x__xgafv=None).execute()
        return self._report_to_dataframe(response)