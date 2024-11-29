import requests
from .mairui_base import MairuiBase

class HSCP(MairuiBase):
    def get_hscp_gsjj(self, code:str):
        """获取公司基本信息和IPO基本信息"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/gsjj/{code}/{self.mairui_token}",
        )
        data = res.json()        
        ##return HSCP_GSJJ(**{**data,"mr_code":mr_code,"t":datetime.today().strftime("%Y-%m-%d %H:%M:%S")})
    
    def get_hscp_sszs(self, code:str):
        """获取公司所属的指数代码和名称"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/sszs/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_ljgg(self, code:str):
        """获取公司历届高管成员名单"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/ljgg/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_ljds(self, code:str):
        """获取公司历届董事成员名单"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/ljds/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_ljjj(self, code:str):
        """获取公司历届监事成员名单"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/ljjj/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_jdlr(self, code:str):
        """获取公司近一年各季度利润"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/jdlr/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data

    def get_hscp_jdxj(self, code:str):
        """获取公司近一年各季度现金流"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/jdxj/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_cwzb(self, code:str):
        """获取公司近一年各季度主要财务指标"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/cwzb/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
        #return list(map(lambda item:FinancialReport(**item),data))
    def get_hscp_sdgd(self, code:str):
        """获取公司十大股东"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/sdgd/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_ltgd(self, code:str):
        """获取公司十大流通股股东"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/ltgd/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_gdbh(self, code:str):
        """获取公司股东变化趋势"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/gdbh/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hscp_jjcg(self, code:str):
        """获取公司最近500家左右的基金持股情况"""
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hscp/jjcg/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
