import json
import pandas as pd
import requests
import tushare

from ylz_utils.config import Config
from ylz_utils.stock.base import StockBase

class TushareStock(StockBase):
    def __init__(self):
        super().__init__()
        self.tushare_token = Config.get('STOCK.TUSHARE.TOKEN')
        self.tuapi = tushare.pro_api(self.tushare_token)
        self.tushare_api_url = "http://api.tushare.pro"

    def _tushare_query(self, api_name, fields='', **kwargs):
        req_params = {
            'api_name': api_name,
            'token': self.tushare_token,
            'params': kwargs,
            'fields': fields
        }
        res = requests.post(
            self.tushare_api_url,
            req_params
        )

        result = json.loads(res.read().decode('utf-8'))

        if result['code'] != 0:
            raise Exception(result['msg'])

        data  = result['data']
        columns = data['fields']
        items = data['items']

        return pd.DataFrame(items, columns=columns)
