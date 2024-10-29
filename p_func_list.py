import os
import io
import concurrent.futures
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox


import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import psutil


# db接続
def db_conn():
    file = os.getcwd() + r'/conf/db_config'
    with io.open(fr'{file}', mode='r', encoding='utf-8') as f:
        lines = f.readlines()
        host = lines[0].strip().replace("host=", "")
        dbname = lines[1].strip().replace("database=", "")
        user = lines[2].strip().replace("user=", "")
        password = lines[3].strip().replace("password=", "")
        port = lines[4].strip().replace("port=", "")
    parameter = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}?client_encoding=utf8',
                           isolation_level="AUTOCOMMIT", poolclass=NullPool)
    pram_d = {"sql": parameter, "main": engine}
    return pram_d

# テキストボックスからSQLインサート
def tb_sql_insert(table_name, col_l, col_l_int, col_l_float ,col_l_date, text_l):
    pram = db_conn()
    txt_l = []
    for n, col in enumerate(col_l):
        value = text_l[n]
        # colがcol_l_intに含まれる場合は整数型へ変換
        if col in col_l_int and value != "":
            txt_l.append(int(float(value)))
        # colがcol_l_floatに含まれる場合は浮動小数点型へ変換
        elif col in col_l_float and value != "":
            txt_l.append(float(value))
        # colがcol_l_dateに含まれる場合はそのまま追加
        elif col in col_l_date and value != "":
            txt_l.append(value)
        else:
            txt_l.append(value if value != "" else None)  # 空白はNoneに変換
    sql = f'INSERT INTO {table_name} ({", ".join(col_l)}) VALUES ({", ".join(["%s"] * len(col_l))})'
    pgdb = psycopg2.connect(pram["sql"])
    cur = pgdb.cursor()
    cur.execute(sql, txt_l)
    pgdb.commit()
    cur.close()
    pgdb.close()

# dataframe高速読み込み_SQL_並列処理
def df_read_sql_con(table_name, col_l, db):
    pram = db_conn()
    def fetch_data(offset):

        # 新しいデータベース接続を各スレッド内で生成
        if db == "main":
            query = f'SELECT {col_l} FROM {table_name} LIMIT {chunksize_max} OFFSET {offset}'
            with conn.connect() as con:
                df_chunk = pd.read_sql_query(sql=text(query), con=con)
        else:
            query = f'''
               SELECT {col_l} FROM (
                   SELECT t.*, ROWNUM rnum
                   FROM {table_name} t
                   WHERE ROWNUM <= {offset + chunksize_max}
               )
               WHERE rnum > {offset}
               '''
            with conn.connect() as con:
                df_chunk = pd.read_sql_query(sql=text(query), con=con)
        return df_chunk
    # db_config選択
    conn = pram["main"]
    if col_l == "":
        col_l = "*"
    # 利用可能なメモリの量を取得
    available_memory = psutil.virtual_memory().available
    # データベースから1行分のデータを読み出し、そのサイズを取得
    df = pd.read_sql_query(sql=text(f'SELECT * FROM {table_name} LIMIT 1'), con=conn.connect())
    if df.empty:
        df = None
        return df
    row_size = df.memory_usage(deep=True).sum()
    # 利用可能なメモリの量に基づいてchunksizeを計算(最大の50%とした)
    chunksize_max = int(available_memory * 0.5 // row_size)
    # 合計行数を取得 (例：テーブル内の全行数)
    total_rows = pd.read_sql_query(f'SELECT COUNT(*) FROM {table_name}', con=conn.connect()).iloc[0, 0]
    # オフセットを生成 (並行して処理するために使う)
    offsets = range(0, total_rows, chunksize_max)
    # 並列処理でデータを取得
    core = int(psutil.cpu_count(logical=True))
    core = core - 2
    if core <= 0:
        core = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=core) as executor:
        # ThreadPoolExecutorで複数タスクを並行実行
        results = list(executor.map(fetch_data, offsets))
    final_df = pd.concat(results, ignore_index=True)
    return final_df

# entry選択カーソル次移動
def focus_next_widget(event):
    event.widget.tk_focusNext().focus()
    return "break"

# テキストボックス生成
def txt_box(txt_box_l):
    window = txt_box_l[0]
    lb_name = txt_box_l[1]
    lb_font_size = int(txt_box_l[2])
    lb_x = int(txt_box_l[3])
    lb_y = int(txt_box_l[4])
    lb_width = int(txt_box_l[5])
    lb_height = int(txt_box_l[6])
    tb_font_size = int(txt_box_l[7])
    tb_x = int(txt_box_l[8])
    tb_y = int(txt_box_l[9])
    tb_width = int(txt_box_l[10])
    tb_height = int(txt_box_l[11])
    lb = ttk.Label(window, text=lb_name, font=("", lb_font_size))
    lb.place(x=lb_x, y=lb_y, width=lb_width, height=lb_height, anchor=tk.NW)
    tb = ttk.Entry(window, font=("", tb_font_size))
    tb.place(x=tb_x, y=tb_y, width=tb_width, height=tb_height, anchor=tk.NW)
    tb.bind("<Return>", focus_next_widget)
    return tb

# テキストボックス値自動削除
def txt_box_val_del(box, times):
    def clear_entry():
        box.delete(0, tk.END)  # Entryの値を削除

    box.after(times, clear_entry)

# tkinter、tree生成
def tk_tree(window, tree_l, x, y ,height, width, df, bind, btn):
    tree = ttk.Treeview(window)
    l_tree_c = 0
    for r in tree_l:
        l_tree_c += 1
    tree["columns"] = (", ".join([f"{i}" for i in range(l_tree_c)]))
    tree["show"] = "headings"
    for i, r in enumerate(tree_l):
        tree.heading(i, text=r, command=lambda:select_header(tree, False))
        tree.column(i, width=100)
    scroll = tk.Scrollbar(tree, orient=tk.VERTICAL, command=tree.yview)
    scroll.pack(side=tk.RIGHT, fill="y")
    tree["yscrollcommand"] = scroll.set
    scroll2 = tk.Scrollbar(tree, orient=tk.HORIZONTAL, command=tree.xview)
    scroll2.pack(side=tk.BOTTOM, fill="x")
    tree["xscrollcommand"] = scroll2.set
    tree.place(x=int(x), y=int(y), height=int(height), width=int(width))
    # ツリーでアイテムが選択されたときにon_select関数を呼び出すように設定
    if bind == "S":
        tree.bind("<<TreeviewSelect>>", btn)
    elif bind == "D":
        tree.bind("<Double-1>", btn)
    elif bind == "O":
        tree.bind("<<TreeviewSelect>>", btn[0])
        tree.bind("<Double-1>", btn[1])
    if df is not None:
        tree.delete(*tree.get_children())
        i = 0
        for r in df.itertuples(index=False):
            tree.insert("", "end", values=r, tags=(str(i),), iid="I" + str(i))
            if i & 1:
                tree.tag_configure(i, background="#CCFFFF")
            i = i + 1
    return tree
# treeソート機能追加
def select_header(tree, reverse_flag):
    # クリックされたヘッダー列取得
    x = tree.winfo_pointerx() - tree.winfo_rootx()
    select_column_str = tree.identify_column(x)
    select_column_int = int(select_column_str[1:]) - 1
    l = [(tree.set(k, select_column_int), k) for k in tree.get_children("")]
    l.sort(reverse=reverse_flag)
    for index, (val, k) in enumerate(l):
        tree.move(k, "", index)
    tree.heading(select_column_int, command=lambda:select_header(tree, not reverse_flag))

# tkinter、dataframeからtree表示
def tk_tree_display(tree, df):
    tree.delete(*tree.get_children())
    i = 0
    for r in df.itertuples(index=False):
        tree.insert("", "end", values=r, tags=(str(i),), iid="I" + str(i))
        if i & 1:
            tree.tag_configure(i, background="#CCFFFF")
        i = i + 1

# tree選択による項目削除
def tree_item_del(mes_txt, t_name, c_name_l, val_no_l, tree, mes):
    pram = db_conn()
    mes.delete(0, tk.END)
    mes.insert(0, "内容削除中です")
    item = tree.focus()
    if item == "":
        mes.delete(0, tk.END)
        mes.insert(0, "削除を行いたい項目が選択されていません")
        return
    answer = messagebox.askyesno("確認", mes_txt)
    if answer is False:
        mes.delete(0, tk.END)
        mes.insert(0, "削除キャンセルしました")
        return
    pgdb = psycopg2.connect(pram["sql"])
    cur = pgdb.cursor()
    selected_items = tree.selection()
    for item in selected_items:
        values = tree.item(item, "values")
        values = list(values)
        if len(c_name_l) == 1:
            cur.execute(f'delete from {t_name} where {c_name_l[0]} = %s;', (values[int(val_no_l[0])],))
        elif len(c_name_l) == 2:
            cur.execute(f'delete from {t_name} where {c_name_l[0]} = %s and {c_name_l[1]} = %s;',
                        (values[int(val_no_l[0])], values[int(val_no_l[1])],))
        elif len(c_name_l) == 3:
            cur.execute(f'delete from {t_name} where {c_name_l[0]} = %s and {c_name_l[1]} = %s and {c_name_l[2]} = %s;',
                        (values[int(val_no_l[0])], values[int(val_no_l[1])], values[int(val_no_l[3])],))
        elif len(c_name_l) == 4:
            cur.execute(f'delete from {t_name} where {c_name_l[0]} = %s and {c_name_l[1]} = %s and {c_name_l[2]} = %s and {c_name_l[3]} = %s;',
                        (values[int(val_no_l[0])], values[int(val_no_l[1])], values[int(val_no_l[2])], values[int(val_no_l[3])],))
        elif len(c_name_l) == 5:
            cur.execute(f'delete from {t_name} where {c_name_l[0]} = %s and {c_name_l[1]} = %s and {c_name_l[2]} = %s and {c_name_l[3]} = %s and {c_name_l[4]} = %s;',
                        (values[int(val_no_l[0])], values[int(val_no_l[1])], values[int(val_no_l[2])], values[int(val_no_l[3])], values[int(val_no_l[4])],))
    pgdb.commit()
    cur.close()
    pgdb.close()
    mes.delete(0, tk.END)
    mes.insert(0, "削除完了です")