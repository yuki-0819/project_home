import os
import datetime
import time
import shutil
import requests
from yt_dlp import YoutubeDL as y_dl


import p_func_list as func

def proc_open():
    table_name = 'public.down_list'
    table_name_dir = 'public.file_dir'
    df = func.df_read_sql_con(table_name, '')
    if df is None:
        exit()
    df = df[(df['down_status'] != "ダウンロード失敗")]
    df = df[(df['down_status'] != "ダウンロード済")]
    if df.empty:
        exit()
    df_dir = func.df_read_sql_con(table_name_dir, '')
    df_dir = df_dir[(df_dir['dir_no'] != "1")]
    addr = ''
    chunk_size = None
    for r in df_dir.itertuples():
        addr = r.dir_addr
        chunk_size = int(float(r.chunk_size))
        break
    #最高の画質と音質を動画をダウンロードする
    ydl_opts = {'format': 'best', 'outtmpl': f'{addr}/%(title)s.%(ext)s'}
    dt = datetime.date.today()
    col_l = ['down_status', 'down_date', 'file_name', 'file_type']
    col_l_int = []
    col_l_float = []
    col_l_date = ['down_date']
    col_l_where = {'item_no': 1}
    for r in df.itertuples():
        if "youtube" in r.url or "xvideos" in r.url or "pornhub" in r.url:
            # 動画のURLを指定
            with y_dl(ydl_opts) as ydl:
                try:
                    info = ydl.extract_info([r.url], download=True)
                    title = info.get('title', 'No Title Available')
                    file_path = ydl.prepare_filename(info)
                    text_l = ["ダウンロード済", dt, title, "Video"]
                except Exception as e:
                    title = "No Title Available"
                    text_l = ["ダウンロード失敗", dt, title, "Video"]
        else:
            # URLからファイル名を取得
            file_name = os.path.basename(r.url)
            # 保存先のフルパスを生成
            save_path = os.path.join(addr, file_name)
            # ファイルをダウンロード
            response = requests.get(r.url, stream=True, timeout=10)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        file.write(chunk)
                #shutil.move(save_path, addr)
                text_l = ["ダウンロード済", dt, file_name, "File"]
            else:
                text_l = ["ダウンロード失敗", dt, response.status_code, "File"]
        val_l_where = [r.item_no]
        func.tb_sql_update(table_name, col_l, col_l_int, col_l_float, col_l_date, col_l_where, val_l_where, text_l)
        time.sleep(5)