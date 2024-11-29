from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import pandas as pd
import requests
from .mairui_base import MairuiBase

class HSRL(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def get_hsrl_ssjy(self,code:str,sync_es:bool=False):
        """获取某个股票的实时交易数据"""
        #数据更新：交易时间段每1分钟
        #请求频率：1分钟600次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        add_fields = {"mr_code":mr_code}
        date_fields = ['t']
        name = f"hsrl_ssjy"
        keys = ["mr_code","t"]
        df = self.load_data(name,f"hsrl/ssjy/{code}",
                                        add_fields=add_fields,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hsrl_mmwp(self,code:str,sync_es:bool=False):
        """获取某个股票的盘口交易数据,返回值没有当前股价，仅有5档买卖需求量价以及委托统计"""
        #数据更新：交易时间段每2分钟
        #请求频率：1分钟300次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        add_fields = {"mr_code":mr_code}
        date_fields = ['t']
        name = f"hsrl_mmwp"
        keys= ["mr_code","t"]
        df = self.load_data(name,f"hsrl/mmwp/{code}",
                                        add_fields=add_fields,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','t'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hsrl_zbjy(self,code:str,sync_es:bool=False):
        """获取某个股票的当天逐笔交易数据"""
        #数据更新：每天20:00开始更新，当日23:59前完成
        #请求频率：1分钟300次
        
        # 字段名称	数据类型	字段说明
        # d	string	数据归属日期（yyyy-MM-dd）
        # t	string	时间（HH:mm:dd）
        # v	number	成交量（股）
        # p	number	成交价
        # ts	number	交易方向（0：中性盘，1：买入，2：卖出）
        
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        mc = code_info['name']
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":mc,"ud":ud}
        date_fields = ['ud']
        name = f"hsrl_zbjy"
        keys = ["mr_code","d","t"]
        sql = f"select * from {name} where mr_code='{mr_code}' and strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hsrl/zbjy/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','d','t'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hsrl_fscj(self,code:str,sync_es=False):
        """获取某个股票的当天分时成交数据"""
        #数据更新：每天20:00开始更新，当日23:59前完成
        #请求频率：1分钟300次
        
        # 字段名称	数据类型	字段说明
        # d	string	数据归属日期（yyyy-MM-dd）
        # t	string	时间（HH:mm:dd）
        # v	number	成交量（股）
        # p	number	成交价
        
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        mc = code_info['name']
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":mc,"ud":ud}
        date_fields = ['ud']
        name = f"hsrl_fscj"
        keys = ["mr_code","d","t"]
        sql = f"select * from {name} where mr_code='{mr_code}' and strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hsrl/fscj/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','d','t'])
            print(f"errors:{es_result["errors"]}")
        return df    
    def get_hsrl_fjcj(self,code:str,sync_es:False):
        """获取某个股票的当天分价成交数据"""
        #数据更新：每天20:00开始更新，当日23:59前完成
        #请求频率：1分钟300次
        
        # 字段名称	数据类型	字段说明
        # d	string	数据归属日期（yyyy-MM-dd）
        # v	number	成交量（股）
        # p	number	成交价
        # b	number	占比（%）
        
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        mc = code_info['name']
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"mc":mc,"ud":ud}
        date_fields = ['ud']
        name = f"hsrl_fjcj"
        keys = ["mr_code","d","t"]
        sql = f"select * from {name} where mr_code='{mr_code}' and strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hsrl/fjcj/{code}",
                                        add_fields=add_fields,
                                        sql=sql,
                                        keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','d','t'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hsrl_zbdd(self,code:str,sync_es:bool=False):
        """获取某个股票的当天逐笔超400手的大单成交数据"""
        #数据更新：每天20:00开始更新，当日23:59前完成
        #请求频率：1分钟300次
        code_info = self._get_stock_code(code)
        code=code_info['code']
        mr_code = code_info['mr_code']
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>21:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"mr_code":mr_code,"ud":ud}
        date_fields = ['ud']
        skip_condition = f"mr_code == '{mr_code}' & (ud.dt.strftime('%Y-%m-%d')='{ud}')"
        name = f"hsrl_zbdd_{mr_code}"
        df = self._load_data(name,f"hsrl/zbdd/{code}",
                                        add_fields=add_fields,
                                        skip_condition=skip_condition,
                                        keys=["mr_code","d"],date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['mr_code','d','t'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def register_router(self):
        @self.router.get("/hsrl/zbjy")
        async def get_hsrl_zbjy(req:Request):
            """获取某个股票的当天逐笔交易数据"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes = [item for item in code.split(',') if item]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]

                kwargs = {
                    "func": self.get_hsrl_zbjy,
                    "codes": codes,
                }
                df_list:list[pd.DataFrame] = self.parallel_execute(**kwargs)
                df = pd.concat(df_list)
                if 'json' in req.query_params.keys():
                    return df.to_dict(orient='records')
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsrl/fscj")
        async def get_hsrl_fscj(req:Request):
            """获取某个股票的当天分时交易数据"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes = [item for item in code.split(',') if item]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]

                kwargs = {
                    "func": self.get_hsrl_fscj,
                    "codes": codes,
                }
                df_list:list[pd.DataFrame] = self.parallel_execute(**kwargs)
                df = pd.concat(df_list)
                if 'json' in req.query_params.keys():
                    return df.to_dict(orient='records')
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hsrl/fjcj")
        async def get_hsrl_fjcj(req:Request):
            """获取某个股票的当天分价交易数据"""
            try:
                code = req.query_params.get('code')
                zx = req.query_params.get('zx')
                bk = req.query_params.get('bk')
                if not code and not zx and not bk:
                    raise Exception('必须指定code或zx或bk参数')
                if code:
                    codes = [item for item in code.split(',') if item]
                elif zx:
                    codes_info = self._get_zx_codes(zx)
                    codes = [item['code'] for item in codes_info]
                elif bk:
                    codes_info = self._get_bk_codes(bk)
                    codes = [item['dm'] for item in codes_info]

                kwargs = {
                    "func": self.get_hsrl_fjcj,
                    "codes": codes,
                }
                df_list:list[pd.DataFrame] = self.parallel_execute(**kwargs)
                df = pd.concat(df_list)
                if 'json' in req.query_params.keys():
                    return df.to_dict(orient='records')
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")