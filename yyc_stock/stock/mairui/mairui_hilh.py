from datetime import datetime, timedelta
from typing import Literal
import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from .mairui_base import MairuiBase

class HILH(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()
    def get_hilh_mrxq(self,sync_es:bool=False):
        '''今日龙虎榜概览'''
        #数据更新：每天15:30（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # t	string	日期yyyy-MM-dd
        # dpl7	string	跌幅偏离值达7%的证券，格式：dpl7:[LhbDetail,...]，LhbDetail对象说明见下表。
        # z20	string	连续三个交易日内，涨幅偏离值累计达20%的证券，格式：z20:[LhbDetail,...]，LhbDetail对象说明见下表。
        # zpl7	string	涨幅偏离值达7%的证券，格式：zpl7:[LhbDetail,...]，LhbDetail对象说明见下表。
        # h20	string	换手率达20%的证券，格式：h20:[LhbDetail,...]，LhbDetail对象说明见下表。
        # st15	string	连续三个交易日内，涨幅偏离值累计达到15%的ST证券、*ST证券和未完成股改证券，格式：st15:[LhbDetail,...]，LhbDetail对象说明见下表。
        # st12	string	连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券，格式：st12:[LhbDetail,...]，LhbDetail对象说明见下表。
        # std15	string	连续三个交易日内，跌幅偏离值累计达到15%的ST证券、*ST证券和未完成股改证券，格式：std15:[LhbDetail,...]，LhbDetail对象说明见下表。
        # std12	string	连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券，格式：std12:[LhbDetail,...]，LhbDetail对象说明见下表。
        # zf15	string	振幅值达15%的证券，格式：zf15:[LhbDetail,...]，LhbDetail对象说明见下表。
        # df15	string	连续三个交易日内，跌幅偏离值累计达20%的证券，格式：df15:[LhbDetail,...]，LhbDetail对象说明见下表。
        # wxz	string	无价格涨跌幅限制的证券，格式：wxz:[LhbDetail,...]，LhbDetail对象说明见下表。
        # wxztp	string	当日无价格涨跌幅限制的A股，出现异常波动停牌的股票，格式：wxztp:[LhbDetail,...]，LhbDetail对象
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>16:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        
        res = requests.get(f"{self.mairui_api_url}/hilh/mrxq/{self.mairui_token}")
        data = res.json()
        print(data.keys())
        data = {k:v for k,v in data.items() if v!=None}
        t = data['t']
        dpl7 = [{'t':t,'type':'dpl7','info':'跌幅偏离值达7%的证券',**item} for item in data.get('dpl7',[])]
        zpl7 = [{'t':t,'type':'zpl7','info':'涨幅偏离值达7%的证券',**item} for item in data.get('zpl7',[])]
        z20 = [{'t':t,'type':'z20','info':'连续三个交易日内，涨幅偏离值累计达20%的证券',**item} for item in data.get('z20',[])]
        h20 = [{'t':t,'type':'h20','info':'换手率达20%的证券',**item} for item in data.get('h20',[])]
        st12 = [{'t':t,'type':'st12','info':'连续三个交易日内，涨幅偏离值累计达到12%的ST证券',**item} for item in data.get('st12',[])]
        st15 = [{'t':t,'type':'st15','info':'连续三个交易日内，涨幅偏离值累计达到15%的ST证券',**item} for item in data.get('st15',[])]
        std12 = [{'t':t,'type':'std12','info':'连续三个交易日内，跌幅偏离值累计达到12%的ST证券',**item} for item in data.get('std12',[])]
        std15 = [{'t':t,'type':'std15','info':'连续三个交易日内，跌幅偏离值累计达到15%的ST证券',**item} for item in data.get('std15',[])]
        zf15 = [{'t':t,'type':'zf15','info':'振幅偏离值累计达到15%的证券',**item} for item in data.get('zf15',[])]
        df15 = [{'t':t,'type':'df15','info':'连续三个交易日内，跌幅偏离值累计达到20%的证券',**item} for item in data.get('df15',[])]
        wxz = [{'t':t,'type':'zf15','info':'无价格涨跌幅限制的证券',**item} for item in data.get('wxz',[])]
        wxztp = [{'t':t,'type':'df15','info':'当日无价格涨跌幅限制的A股，出现异常波动停牌的股票',**item} for item in data.get('wxztp',[])]
        
        return pd.DataFrame(dpl7+zpl7+z20+h20+st12+st15+std12+std15+zf15+df15+wxz+wxztp)
    def get_hilh_ggsb(self,days:Literal[5,10,30,60]=5,sync_es:bool=False):   
        """近n日上榜个股，其中 近n日 中的n可以是 5、10、30、60 。"""
        #http://api.mairui.club/hilh/ggsb/近几日(如5)/您的licence
        #数据更新：每天15:30（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # count	number	上榜次数
        # totalb	number	累积获取额(万)
        # totals	number	累积卖出额(万)
        # netp	number	净额(万)
        # xb	number	买入席位数
        # xs	number	卖出细微

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>16:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"days":days,"ud":ud}
        date_fields = ['ud']
        keys=['dm','days','ud']
        name = f"hilh_ggsb"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud) = '{ud}' and days={days}"
        
        df = self.load_data(name,f"hilh/ggsb/{days}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','days','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def get_hilh_yybsb(self,days:Literal[5,10,30,60]=5,sync_es:bool=False):   
        """近n日营业部上榜统计，其中 近n日 中的n可以是 5、10、30、60 。"""
        #数据更新：每天15:30（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # yybmc	string	营业部名称
        # count	number	上榜次数
        # totalb	number	累积获取额(万)
        # bcount	number	买入席位
        # totals	number	累积卖出额(万)
        # scount	number	卖出席位
        # top3	string	买入前三股票

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>16:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"days":days,"ud":ud}
        date_fields = ['ud']
        keys=['yybmc','days','ud']
        name = f"hilh_yybsb"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud) = '{ud}' and days={days}"
        
        df = self.load_data(name,f"hilh/yybsb/{days}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['yybmc','days','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hilh_jgxw(self,days:Literal[5,10,30,60]=5,sync_es:bool=False):   
        """近n日个股机构交易追踪，其中 近n日 中的n可以是 5、10、30、60 。"""
        #数据更新：每天15:30（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # dm	string	股票代码
        # mc	string	股票名称
        # be	number	累积买入额(万)
        # bcount	number	买入次数
        # se	number	累积卖出额(万)
        # scount	number	卖出次数
        # ende	number	净额(万)

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>16:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"days":days,"ud":ud}
        date_fields = ['ud']
        keys=['dm','days','ud']
        name = f"hilh_jgxw"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud) = '{ud}' and days={days}"
        
        df = self.load_data(name,f"hilh/jgxw/{days}",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','days','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hilh_xwmx(self,sync_es:bool=False):   
        """获取近五个交易日（按交易日期倒序）上榜个股被机构交易的总额，以及个股上榜原因。"""
        #数据更新：每天15:30（约10分钟完成更新）
        #请求频率：1分钟20次 

        # 字段名称	数据类型	字段说明
        # m	string	股票代码
        # mc	string	股票名称
        # t	string	交易日期yyyy-MM-dd
        # buy	number	机构席位买入额(万)
        # sell	number	机构席位卖出额(万)
        # type	number	类型

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>16:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hilh_xwmx"
        
        df = self.load_data(name,f"hilh/xwmx",
                            add_fields=add_fields,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def register_router(self):
        @self.router.get("/hilh/mrxq",response_class=HTMLResponse)
        async def get_hilh_mrxq(req:Request):
            """今日龙虎榜概览"""
            try:
                df = self.get_hilh_mrxq()
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/hilh/ggsb/{days}",response_class=HTMLResponse)
        async def get_hilh_ggsb(days:int,req:Request):
            """近n日上榜个股，其中 近n日 中的n可以是 5、10、30、60 。"""
            try:
                df = self.get_hilh_ggsb(days)
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        
        @self.router.get("/hilh/yybsb/{days}",response_class=HTMLResponse)
        async def get_hilh_yybsb(days:int,req:Request):
            """近n日营业部上榜统计，其中 近n日 中的n可以是 5、10、30、60 。"""
            try:
                df = self.get_hilh_yybsb(days)
                df = self._prepare_df(df,req)
                content = self._to_html(df)
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
            
        @self.router.get("/hilh/jgxw/{days}",response_class=HTMLResponse)
        async def get_hilh_mrxq(days:int,req:Request):
            """近n日个股机构交易追踪，其中 近n日 中的n可以是 5、10、30、60 。"""
            try:
                df = self.get_hilh_jgxw(days)
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
                
        @self.router.get("/hilh/xwmx",response_class=HTMLResponse)
        async def get_hilh_xwmx(req:Request):
            """获取近五个交易日（按交易日期倒序）上榜个股被机构交易的总额，以及个股上榜原因。"""
            try:
                df = self.get_hilh_xwmx()
                df = self._prepare_df(df,req)
                content = df.to_html()
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")