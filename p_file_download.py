import tkinter as tk
from tkinter import ttk
import datetime


import p_func_list as func

def insert_btn():
    mes.delete(0, tk.END)
    mes.insert(0, "追加処理中です")
    col_l_int = []
    col_l_float = []
    col_l_date = []
    dt = datetime.date.today()
    year = dt.strftime('%Y')[2:4]
    month = dt.strftime('%m')
    order = "NO" + str(year) + str(month)
    df = func.df_read_sql_con(table_name, "", "main")
    if df is None:
        item_no = order + "-000001"
    else:
        df['order_year_month'] = df['item_no'].str.slice(0, 6)
        df = df[(df['order_year_month'] == order)]
        if df.empty:
            item_no = order + "-000001"
        else:
            df = df.fillna("").sort_values(by=['item_no'], ascending=[False])
            no = None
            for r in df.itertuples():
                no = r.item_no
                break
            no = int(no[7:13])
            if no < 9:
                item_no = order + "-00000" + str(no + 1)
            elif no < 99:
                item_no = order + "-0000" + str(no + 1)
            elif no < 999:
                item_no = order + "-000" + str(no + 1)
            elif no < 9999:
                item_no = order + "-00" + str(no + 1)
            elif no < 99999:
                item_no = order + "-0" + str(no + 1)
            else:
                item_no = order + "-" + str(no + 1)
    dt = datetime.datetime.now()
    text_l = [item_no, tb_1.get(), dt, "未ダウンロード", None, None, None]
    func.tb_sql_insert(table_name, col_l, col_l_int, col_l_float ,col_l_date, text_l)
    df = func.df_read_sql_con(table_name, "", "main")
    if df is not None:
        df = df.fillna("")
        func.tk_tree_display(tree, df)
    else:
        tree.delete(*tree.get_children())
    tb_1.delete(0, tk.END)
    mes.delete(0, tk.END)
    mes.insert(0, "追加処理が完了しました")
    func.txt_box_val_del(mes, 2000)

def delete_btn():
    mes_txt = "選択されている項目を削除しますが、よろしいですか？"
    c_name_l = ['item_no']
    val_no_l = [0]
    func.tree_item_del(mes_txt, table_name, c_name_l, val_no_l, tree, mes)
    df = func.df_read_sql_con(table_name, "", "main")
    if df is not None:
        df = df.fillna("")
        func.tk_tree_display(tree, df)
    else:
        tree.delete(*tree.get_children())
    func.txt_box_val_del(mes, 2000)

def clip_get():
    clipboard_text = main_win.clipboard_get()
    if clipboard_text == "":
        return
    if "http" in clipboard_text:
        col_l_int = []
        col_l_float = []
        col_l_date = []
        dt = datetime.date.today()
        year = dt.strftime('%Y')[2:4]
        month = dt.strftime('%m')
        order = "NO" + str(year) + str(month)
        df = func.df_read_sql_con(table_name, "", "main")
        if df is None:
            item_no = order + "-000001"
        else:
            df['order_year_month'] = df['item_no'].str.slice(0, 6)
            df = df[(df['order_year_month'] == order)]
            if df.empty:
                item_no = order + "-000001"
            else:
                df = df.fillna("").sort_values(by=['item_no'], ascending=[False])
                no = None
                for r in df.itertuples():
                    no = r.item_no
                    break
                no = int(no[7:13])
                if no < 9:
                    item_no = order + "-00000" + str(no + 1)
                elif no < 99:
                    item_no = order + "-0000" + str(no + 1)
                elif no < 999:
                    item_no = order + "-000" + str(no + 1)
                elif no < 9999:
                    item_no = order + "-00" + str(no + 1)
                elif no < 99999:
                    item_no = order + "-0" + str(no + 1)
                else:
                    item_no = order + "-" + str(no + 1)
        dt = datetime.datetime.now()
        text_l = [item_no, clipboard_text, dt, "未ダウンロード", None, None, None]
        func.tb_sql_insert(table_name, col_l, col_l_int, col_l_float, col_l_date, text_l)
        df = func.df_read_sql_con(table_name, "", "main")
        if df is not None:
            df = df.fillna("")
            func.tk_tree_display(tree, df)
        else:
            tree.delete(*tree.get_children())
        mes.delete(0, tk.END)
        mes.insert(0, "クリップボードから取得・追加しました")
        func.txt_box_val_del(mes, 2000)
    else:
        mes.delete(0, tk.END)
        mes.insert(0, "クリップボードの値がインターネットのURLではありません")
        func.txt_box_val_del(mes, 2000)

main_win = tk.Tk()
main_win.title("ファイルダウンローダー")
main_win.geometry("1920x1080")
main_frm = ttk.Frame(main_win)
main_frm.place(x=0, y=0, width=1920, height=1080)
table_name = "public.down_list"
col_l = ["item_no", "url", "registration_date", "down_status", "file_name", "file_type", "down_date"]
l_tree = ["No", "ダウンロードURL", "登録日", "ステータス", "ファイル名", "ファイルタイプ", "ダウンロード日"]
df = func.df_read_sql_con(table_name, "", "main")
if df is not None:
    df = df.fillna("")
tree = func.tk_tree(main_frm, l_tree, 10, 10, 850, 1900, df, None, None)
txt_box_l = [main_frm, "URL：", 15, 10, 950, 80, 30, 15, 100, 950, 1800, 30]
tb_1 = func.txt_box(txt_box_l)
sub_btn1 = ttk.Button(main_frm, text="追加", width=100, command=insert_btn)
sub_btn1.place(x=1800, y=1000, width=100, height=30, anchor=tk.NW)
sub_btn2 = ttk.Button(main_frm, text="削除", width=100, command=delete_btn)
sub_btn2.place(x=10, y=1000, width=100, height=30, anchor=tk.NW)
sub_btn3 = ttk.Button(main_frm, text="クリップボード取得", width=150, command=clip_get)
sub_btn3.place(x=1750, y=910, width=150, height=30, anchor=tk.NW)
mes = ttk.Entry(main_frm, font=("", 15))
mes.place(x=10, y=1040, width=1900, height=30, anchor=tk.NW)
main_win.columnconfigure(0, weight=1)
main_win.rowconfigure(0, weight=1)
main_frm.columnconfigure(1, weight=1)
main_win.resizable(False, False)
main_win.mainloop()
