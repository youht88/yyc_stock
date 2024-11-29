
import asyncio
import logging
import argparse
from argparse import Namespace

from yyc_stock.cli.init import init

from yyc_stock.cli.reset import reset
from yyc_stock.cli.stock import stock

def run():
    main()

def main():
    usage= \
"""
    examples:
        # 初始化配置信息 
        ylz_stock reset 

        # 启动stock
        ylz_stock stock 
"""
    parser = argparse.ArgumentParser(description = "测试工具",usage=usage)
    parser.add_argument("--project_name",type=str,default="ylz_stock",help="project名称")
    parser.add_argument("--config_name",type=str,default="config.yaml",help="config名称")
    parser.add_argument("--log_level",type=str,default="INFO",choices=["INFO","DEBUG"],help="日志级别,默认:INFO")
    parser.add_argument("--log_name",type=str,default="ylz_stock.log",help="日志文件名称")
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="可以使用的子命令")
    
    reset_parser = subparsers.add_parser("reset", help="执行初始化")
    
    stock_parser = subparsers.add_parser("stock", help="测试stock")
    stock_parser.add_argument("--host",type=str,help="host")
    stock_parser.add_argument("--port",type=int,help="port")
    
    args:Namespace = parser.parse_args()

    #print("args====>",args)

    if args.command =="reset":
        reset(args)
        return
    
    init(args)
   
    if args.command == "stock":
        stock(args)
            
if __name__ == "__main__":
   main()
