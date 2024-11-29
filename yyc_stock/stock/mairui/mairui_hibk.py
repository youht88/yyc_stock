from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
import requests
from .mairui_base import MairuiBase

class HIBK(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def get_hibk_zjhhy(self,sync_es=False):
        """获取所有证监会定义的行业板块个股统计数据"""
        #数据更新：每天15:30（约10分钟更新完成）
        #请求频率：1分钟20次
        
        # 字段名称	数据类型	字段说明
        # t	string	服务器更新时间yyyy-MM-dd HH:mm:ss
        # mc	string	行业名称
        # dm	string	行业代码
        # jj	number	均价（元）
        # zdf	number	涨跌幅（%）
        # lrzj	number	流入资金（元）
        # lczj	number	流出资金（元）
        # jlr	number	净流入（元）
        # jlrl	number	净流入率（%）
        # lzgmc	string	领涨股名称
        # lzgdm	string	领涨股代码
        # lzgjlrl	number	领涨股净流入率（%）
        
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hibk_zjhhy"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hibk/zjhhy",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hibk_gnbk(self,sync_es=False):
        """获取所有概念板块个股统计数据"""
        #数据更新：每天15:30（约10分钟更新完成）
        #请求频率：1分钟20次

        # 字段名称	数据类型	字段说明
        # t	string	服务器更新时间yyyy-MM-dd HH:mm:ss
        # mc	string	概念板块名称
        # dm	string	概念板块代码
        # jj	number	均价（元）
        # zdf	number	涨跌幅（%）
        # lrzj	number	流入资金（元）
        # lczj	number	流出资金（元）
        # jlr	number	净流入（元）
        # jlrl	number	净流入率（%）
        # lzgmc	string	领涨股名称
        # lzgdm	string	领涨股代码
        # lzgjlrl	number	领涨股净流入率（%）

        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hibk_gnbk"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hibk/gnbk",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,
                            date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def register_router(self):
        @self.router.get("/hibk/zjhhy")
        async def get_hibk_zjhhy(req:Request):
            """证监会行业资金流向，根据流入资金倒序排列"""
            try:
                df = self.get_hibk_zjhhy()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc','lzgdm','lzgmc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
        @self.router.get("/hibk/gnbk")
        async def get_hibk_gnbk(req:Request):
            """概念板块资金流向，根据流入资金倒序排列"""
            try:
                df = self.get_hibk_gnbk()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc','lzgdm','lzgmc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")