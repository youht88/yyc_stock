from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests
from .mairui_base import MairuiBase

class HSMY(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def get_hsmy_zlzj(self,code:str):
        """获取某个股票的每分钟主力资金走势"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        name = code_info['name']
        add_fields = {"mr_code":mr_code,"mc":name,'ud':ud}
        date_fields = ['t','ud']
        name = 'hsmy_zlzj'
        sql = f"delete from {name} where mr_code='{mr_code}' and strftime('%Y-%m-%d',ud)='{ud}'"
        keys=['mr_code','t']
        df = self.load_data(name,f"hsmy/zlzj/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        return df
    def get_hsmy_zjlr(self,code:str):
        """获取某个股票的近十年每天资金流入趋势"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        add_fields = {"mr_code":mr_code,"ud":ud}
        name = "hsmy_zjlr"
        sql = f"delete from {name} where mr_code='{mr_code}' and strftime('%Y-%m-%d',ud)='{ud}'"
        keys=['mr_code','t']
        date_fields = ['t','ud']
        df = self.load_data(name,f"hsmy/zjlr/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        return df

    def get_hsmy_zhlrt(self,code:str,sync_es:bool=False):
        """获取某个股票的近10天资金流入趋势"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        name = code_info['name']
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":name,"ud":ud}
        date_fields = ['t','ud']
        keys=["mr_code","t","ud"]
        name = f"hsmy_zhlrt"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and mr_code='{mr_code}'"
        df = self.load_data(name,f"hsmy/zhlrt/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hsmy_jddx(self,code:str):
        """获取某个股票的近十年主力阶段资金动向"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        res = requests.get( 
            f"{self.mairui_api_url}/hsmy/jddx/{code}/{self.mairui_token}",
        )
        data = res.json()        
        return data
    def get_hsmy_jddxt(self,code:str,sync_es=False):
        """获取某个股票的近十天主力阶段资金动向"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        name = code_info['name']
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":name,"ud":ud}
        date_fields = ['t','ud']
        keys=["mr_code","t","ud"]
        name = f"hsmy_jddxt"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and mr_code='{mr_code}'"
        df = self.load_data(name,f"hsmy/jddxt/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hsmy_lscj(self,code:str,sync_es=False):
        """获取某个股票的近十年每天历史成交分布"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        name = code_info['name']
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":name,"ud":ud}
        date_fields = ['t','ud']
        keys=["mr_code","t","ud"]
        name = f"hsmy_lscj"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and mr_code='{mr_code}'"
        df = self.load_data(name,f"hsmy/lscj/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hsmy_lscjt(self,code:str,sync_es=False):
        """获取某个股票的近十天历史成交分布"""
        #数据更新：每天20:00开始更新（更新耗时约4小时）
        #请求频率：1分钟300次 
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        name = code_info['name']
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":name,"ud":ud}
        date_fields = ['t','ud']
        keys=["mr_code","t","ud"]
        name = f"hsmy_lscjt"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}' and mr_code='{mr_code}'"
        df = self.load_data(name,f"hsmy/lscjt/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def register_router(self):
        @self.router.get("/hsmy/zlzj/{code}",response_class=HTMLResponse)
        async def get_hsmy_zlzj(code:str,req:Request):
            """获取某个股票的每分钟主力资金走势"""
            try:
                df = self.get_hsmy_zlzj(code)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsmy/zhlrt/{code}",response_class=HTMLResponse)
        async def get_hsmy_zhlrt(code:str,req:Request):
            """获取某个股票的近10天资金流入趋势"""
            try:
                df = self.get_hsmy_zhlrt(code)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsmy/jddxt/{code}",response_class=HTMLResponse)
        async def get_hsmy_jddxt(code:str,req:Request):
            """获取某个股票的近十天主力阶段资金动向"""
            try:
                df = self.get_hsmy_jddxt(code)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsmy/lscjt/{code}",response_class=HTMLResponse)
        async def get_hsmy_lscjt(code:str,req:Request):
            """获取某个股票的近十天历史成交分布"""
            try:
                df = self.get_hsmy_lscjt(code)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsmy/lscj/{code}")
        async def get_hsmy_lscj(code:str,req:Request):
            """获取某个股票的近十年历史成交分布"""
            try:
                df = self.get_hsmy_lscj(code)
                if "json" in req.query_params.keys():
                    return df.to_dict(orient="records")
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")