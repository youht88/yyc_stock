from fastapi.responses import JSONResponse
from ylz_utils.config import Config 

from ylz_utils.loger import LoggerLib
from fastapi import FastAPI, HTTPException
import fastapi_cdn_host

from yyc_stock.stock.akshare import *
from yyc_stock.stock.snowball import *
from yyc_stock.stock.mairui import *
from yyc_stock.stock.base import *
class StockLib():    
    def __init__(self):
        self.base = StockBase()
        self.snowball = SnowballStock()
        self.ak_base = AkshareBase()
        self.ak_refresh = AK_REFRESH()
        self.ak_current = AK_CURRENT()
        self.ak_hist = AK_HIST()
        self.ak_flow = AK_FLOW()
        self.ak_rank = AK_RANK()
        self.ak_lhb = AK_LHB()
        self.hibk = HIBK()
        self.hijg = HIJG()
        self.higg = HIGG()
        self.himk = HIMK()
        self.hitc = HITC()
        self.hilh = HILH()
        self.hizj = HIZJ()
        self.hscp = HSCP()
        self.hslt = HSLT()
        self.hsmy = HSMY()
        self.hsrl = HSRL()
        self.hszb = HSZB()
        self.hszg = HSZG()
        self.zs = ZS()     

    def register_app(self,app:FastAPI): 
        self.base.register_router()
        app.include_router(self.base.router,prefix="/base")

        app.include_router(self.hibk.router,prefix="/mairui")
        app.include_router(self.hijg.router,prefix="/mairui")
        app.include_router(self.higg.router,prefix="/mairui")
        app.include_router(self.himk.router,prefix="/mairui")
        app.include_router(self.hsrl.router,prefix="/mairui")
        app.include_router(self.hsmy.router,prefix="/mairui")
        app.include_router(self.hilh.router,prefix="/mairui")
        app.include_router(self.hizj.router,prefix="/mairui")
        app.include_router(self.hszb.router,prefix="/mairui")
        app.include_router(self.hszg.router,prefix="/mairui")

        app.include_router(self.snowball.router,prefix="/snowball")

        app.include_router(self.ak_base.router,prefix="/ak")
        app.include_router(self.ak_refresh.router,prefix="/ak")
        app.include_router(self.ak_current.router,prefix="/ak")
        app.include_router(self.ak_hist.router,prefix="/ak")
        app.include_router(self.ak_flow.router,prefix="/ak")
        app.include_router(self.ak_rank.router,prefix="/ak")
        app.include_router(self.ak_lhb.router,prefix="/ak")

if __name__ == "__main__":
    import uvicorn

    Config.init('ylz_utils')
    logger = LoggerLib.init('ylz_utils')
    
    # 创建 FastAPI 实例
    app = FastAPI()
    fastapi_cdn_host.patch_docs(app)
    
    stockLib = StockLib()
    stockLib.register_app(app)

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request, exc):
        return JSONResponse(content={"error": exc.detail}, status_code=exc.status_code)

    # 创建一个路由，定义路径和处理函数
    uvicorn.run(app, host="127.0.0.1", port=8000)
        
    