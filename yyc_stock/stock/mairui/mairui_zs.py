from datetime import datetime, timedelta
from typing import Literal
import requests
from .mairui_base import MairuiBase

class ZS(MairuiBase):
    def get_zs_all(self):
        """获取沪深两市的主要指数列表"""
        res = requests.get( 
            f"{self.mairui_api_url}/zs/all/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_zs_sh(self):
        """获取沪市的指数列表"""
        res = requests.get( 
            f"{self.mairui_api_url}/zs/sh/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_zs_all(self):
        """获取深市的指数列表"""
        res = requests.get( 
            f"{self.mairui_api_url}/zs/sz/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_zs_sssj(self,code:str):
        """获取某个指数的实时交易数据"""
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/zs/sssj/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    
    def get_zs_lsgl(self):
        """获取沪深两市不同涨跌幅的股票数统计"""
        #数据更新：交易时间段每2分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        res = requests.get( 
            f"{self.mairui_api_url}/zs/lsgl/{self.mairui_token}",
        )
        data = res.json()        
        return data
    
    def get_zs_fsjy(self,code:str,fs:Literal["5m","15m","30m","60m","dn","wn","mn","yn"]="5m"):
        """获取指数代码分时交易实时数据。分时级别支持5分钟、15分钟、30分钟、60分钟、日周月年级别，对应的值分别是 5m、15m、30m、60m、dn、wn、mn、yn """
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/zs/fsjy/{code}/{fs}/{self.mairui_token}",
        )
        data = res.json()        
        return data

    def get_zs_ma(self,code:str,fs:Literal["5m","15m","30m","60m","dn","wn","mn","yn"]="5m"):
        """获取指数代码分时交易的平均移动线数据。分时级别支持5分钟、15分钟、30分钟、60分钟、日周月年级别，对应的值分别是 5m、15m、30m、60m、dn、wn、mn、yn """
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/zs/ma/{code}/{fs}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    
    def get_zs_hfsjy(self,code:str,fs:Literal["5m","15m","30m","60m","dn","wn","mn","yn"]="5m"):
        """获取指数代码分时交易历史数据。分时级别支持5分钟、15分钟、30分钟、60分钟、日周月年级别，对应的值分别是 5m、15m、30m、60m、dn、wn、mn、yn """
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/zs/hfsjy/{code}/{fs}/{self.mairui_token}",
        )
        data = res.json()        
        return data

    def get_zs_hma(self,code:str,fs:Literal["5m","15m","30m","60m","dn","wn","mn","yn"]="5m"):
        """获取指数代码分时交易的平均移动线历史数据。分时级别支持5分钟、15分钟、30分钟、60分钟、日周月年级别，对应的值分别是 5m、15m、30m、60m、dn、wn、mn、yn """
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/zs/hma/{code}/{fs}/{self.mairui_token}",
        )
        data = res.json()        
        return data
