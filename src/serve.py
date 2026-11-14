import http.server,functools,os
root=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
h=functools.partial(http.server.SimpleHTTPRequestHandler,directory=root)
http.server.ThreadingHTTPServer(("127.0.0.1",8765),h).serve_forever()
