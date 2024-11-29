from datetime import datetime, timedelta
import requests

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from .mairui_base import MairuiBase

class HIZJ(MairuiBase):
    def __init__(self):
        super().__init__()
        self.register_router()

    def get_hizj_zjh(self,sync_es:bool=False):   
        """获取近3、5、10天证监会行业资金流入情况"""
        #数据更新：每天15:30开始更新（更新耗时约10分钟）
        #请求频率：1分钟20次 
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        print("run get_hizj_zjh",today)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hizj_zjh"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"        
        df = self.load_data(name,f"hizj/zjh",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    def get_hizj_bk(self,sync_es:bool=False):   
        """获取近3、5、10天概念板块资金流入情况"""
        #数据更新：每天15:30开始更新（更新耗时约10分钟）
        #请求频率：1分钟20次 
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hizj_bk"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hizj/bk",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def get_hizj_ggzl(self,sync_es:bool=False):   
        """个股阶段净流入资金统计总览"""
        #数据更新：每天15:30开始更新（更新耗时约10分钟）
        #请求频率：1分钟20次 
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hizj_ggzl"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hizj/ggzl",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def get_hizj_lxlr(self,sync_es:bool=False):   
        """获取主力连续净流入统计"""
        #数据更新：每天15:30开始更新（更新耗时约10分钟）
        #请求频率：1分钟20次 
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hizj_lxlr"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"       
        df = self.load_data(name,f"hizj/lxlr",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df

    def get_hizj_lxlc(self,sync_es:bool=False):   
        """获取主力连续净流出统计"""
        #数据更新：每天15:30开始更新（更新耗时约10分钟）
        #请求频率：1分钟20次 
        today = datetime.today()
        yestoday = datetime.today() - timedelta(days=1)
        if today.hour>15:
            ud = today.strftime("%Y-%m-%d")
        else:
            ud = yestoday.strftime("%Y-%m-%d")
        add_fields = {"ud":ud}
        date_fields = ['t','ud']
        keys=['dm','ud']
        name = f"hizj_lxlc"
        sql = f"select * from {name} where strftime('%Y-%m-%d',ud)='{ud}'"
        df = self.load_data(name,f"hizj/lxlc",
                            add_fields=add_fields,
                            sql=sql,
                            keys=keys,date_fields=date_fields)
        if sync_es:
            es_result = self.esLib.save(name,df,ids=['dm','ud'])
            print(f"errors:{es_result["errors"]}")
        return df
    
    def register_router(self):
        @self.router.get("/refresh_hizj")
        async def refresh_hizj():
            """刷新资金流入情况数据,需要同步es"""
            try:
                funcs = [self.get_hizj_bk,self.get_hizj_zjh,self.get_hizj_ggzl,self.get_hizj_lxlr,self.get_hizj_lxlc]
                for func in funcs:
                    func(sync_es = False)
                return {"message":"refresh_hizj completed!"}
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
            
        @self.router.get("/hizj/ggzl")
        async def get_hizj_ggzl(req:Request):
            """个股阶段净流入资金统计总览"""
            try:
                df = self.get_hizj_ggzl()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
             
        @self.router.get("/hizj/bk",response_class=HTMLResponse)
        async def get_hizj_bk(req:Request):
            """获取近3、5、10天概念板块资金流入情况"""
            try:
                df = self.get_hizj_bk()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
             
        @self.router.get("/hizj/zjh",response_class=HTMLResponse)
        async def get_hizj_zjh(req:Request):
            """获取近3、5、10天证监会行业资金流入情况"""
            try:
                df = self.get_hizj_zjh()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}") 
            
        @self.router.get("/hizj/lxlr",response_class=HTMLResponse)
        async def get_hizj_lxlr(req:Request):
            """获取主力连续净流入统计"""
            try:
                df = self.get_hizj_lxlr()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")        
        
        @self.router.get("/hizj/lxlc",response_class=HTMLResponse)
        async def get_hizj_lxlc(req:Request):
            """获取主力连续净流出统计"""
            try:
                df = self.get_hizj_lxlc()
                df = self._prepare_df(df,req)
                content = self._to_html(df,columns=['dm','mc'])
                return HTMLResponse(content=content)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"{e}")
