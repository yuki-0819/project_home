import datetime
import time

import a_file_download


while True:
    now = datetime.datetime.now()
    print("処理開始：" + str(now))
    if now.hour == 4 and 00 <= now.minute <= 59:
        break  # 4:00から4:59になったらループを終了する
    else:
        a_file_download.proc_open()
    print("処理待機：" + str(datetime.datetime.now()))
    time.sleep(300)  # 5 分待つ