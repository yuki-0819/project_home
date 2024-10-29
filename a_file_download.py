from yt_dlp import youtubedl

#最高の画質と音質を動画をダウンロードする
ydl_opts = {'format': 'best'}

#動画のURLを指定
with youtubedl(ydl_opts) as ydl:
    ydl.download(['hhttps://youtu.be/5wEtefq9VzM'])