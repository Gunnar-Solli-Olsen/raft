import sys
import time
from urllib.parse import urlparse
import http.server
import socketserver
import signal
import socket
import threading

import requests # for heartbeat monitoring thread

try:
    output_id, address, nodes_list = sys.argv[1], sys.argv[2], sys.argv[3:]
    leader = nodes_list[0]
    if (address == leader):
        im_leader = True
    else:
        im_leader = False
except IndexError:
    print("Usage: log-server.py <host:port>")
    sys.exit(1)

crashed = False
local_log = []

previous_heartbeat = time.time()
election = False

# 
class HeartbeatMonitor(threading.Thread):

    def run(self):
        global previous_heartbeat, election, im_leader
        timeout = 2.0
        while True:
            if (im_leader):
                time.sleep(timeout/2)
                # send heartbeats
                #print("sending heartbeats")
                for a in nodes_list:
                    if (a != address):
                        try:
                            url = f"http://{a}/heartbeat"
                            response = requests.post(url)
                        except requests.exceptions.RequestException as e:
                            print(f"Failed to send heartbeat to {a}")
            else:
                time.sleep(timeout)
                if (time.time() - previous_heartbeat > timeout):
                    print("Heartbeat timeout detected") # We need to set election here

class LogRequestHandler(http.server.SimpleHTTPRequestHandler):

    def do_PUT(self):
        global crashed, local_log
        
        if crashed:
            print(f"\n{self.server.server_address} Received PUT request while crashed, ignoring\n")
            return
        
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8')
        print(f"{self.server.server_address} Received PUT request with data: {data}")
        
        # Current logging logic is simple: it just appends the data to a list.
        local_log.append(data)

        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        global crashed, local_log
        url = urlparse(self.path).path

        # If POST is extended, this case should be kept intact and overrule other URLs.
        if crashed and url != "/crash" and url != "/recover" and url != "/exit":
            print(f"\n{self.server.server_address} Received POST request while crashed, ignoring\n")
            return

        if url == "/crash":
            print(f"{self.server.server_address} Simulating crash...")
            crashed = True
            self.send_response(200)
            self.end_headers()
            
        elif url == "/recover":
            print(f"{self.server.server_address} Simulating recovery...")
            crashed = False
            self.send_response(200)
            self.end_headers()

        elif url == "/exit":
            print(f"{self.server.server_address} Exiting...")
            self.send_response(200)
            self.end_headers()
            print(f"{self.server.server_address}: {local_log}")
            with open(f"output/{output_id}-server-{self.server.server_address[0]}{self.server.server_address[1]}.csv", 'w') as f:
                for entry in local_log:
                    f.write(f"{entry}\n")

        elif url == "/heartbeat":
            global previous_heartbeat
            previous_heartbeat = time.time()
            self.send_response(200)
            self.end_headers()
        
def start_server(address):

    print("starting heartbeat monitor")
    heartbeat_monitor = HeartbeatMonitor()
    heartbeat_monitor.daemon = True
    heartbeat_monitor.start()

    print("starting server")
    host, port = address.split(':')
    with socketserver.TCPServer((host, int(port)), LogRequestHandler) as server:
        print(f"Serving HTTP on {host} port {port}...")
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.serve_forever()

if __name__ == "__main__":
    start_server(address)