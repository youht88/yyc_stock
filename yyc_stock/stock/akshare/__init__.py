from fastapi import FastAPI

from yyc_stock.stock.akshare.ak_base import AkshareBase
from yyc_stock.stock.akshare.ak_refresh import AK_REFRESH
from yyc_stock.stock.akshare.ak_current import AK_CURRENT
from yyc_stock.stock.akshare.ak_hist import AK_HIST
from yyc_stock.stock.akshare.ak_flow import AK_FLOW
from yyc_stock.stock.akshare.ak_rank import AK_RANK
from yyc_stock.stock.akshare.ak_lhb import AK_LHB
from yyc_stock.stock.akshare.ak_pankou import AK_PANKOU
from yyc_stock.stock.akshare.ak_torch import AK_TORCH
