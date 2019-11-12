""" 负责数据采集、清洗、转换和持久化的任务器
由于需要下载的数据巨大且会每日更新，服务器网站限流，。
"""
from src import tushare_data as dft

if __name__ == "__main__":
    dft.get_process_tushare_list()