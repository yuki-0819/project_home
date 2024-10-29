from yt_dlp import YoutubeDL as y_dl

#最高の画質と音質を動画をダウンロードする
ydl_opts = {'format': 'best', 'outtmpl': '%(title)s.%(ext)s'}

#動画のURLを指定
with y_dl(ydl_opts) as ydl:
    ydl.download(['hhttps://youtu.be/5wEtefq9VzM'])