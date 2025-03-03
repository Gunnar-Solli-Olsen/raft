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
# local_log = {} # this is to preserve index and term easier?
new_log = ()
term = 1
index = 0

previous_heartbeat = time.time()
election = False


class HeartbeatMonitor(threading.Thread):

    def run(self):
        global previous_heartbeat, election, im_leader
        timeout = 2.0 # this could be changed 
        while True:
            if (im_leader):
                time.sleep(timeout/2) # this could be changed for some other number
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
                    print("Heartbeat timeout detected") # We need to activate election here


class ConnectionHandler(threading.Thread):

    def run(self):
        data = self._args[0]
        global nodes_list
        node_count = len(nodes_list)
        append_confirms = 1 # 1 vote from leader  

        for a in nodes_list:
            if (a == leader):
                continue
            print(f"leader sending append request to {a}...")
            try:  
                url = f"http://{a}/append" # leader sends append request to followers including the data 
                response = requests.put(url, data=data.encode('utf-8'))
                if response.status_code == 200:
                    append_confirms += 1

            except requests.exceptions.Timeout:
                print(f"Request to {a} timed out >:(")

        if (append_confirms > node_count/2):
            print("Majority of nodes have appended the log entry")
            # commit the log entry


class LogRequestHandler(http.server.SimpleHTTPRequestHandler):

    def do_PUT(self):
        global crashed, local_log
        url = urlparse(self.path).path
        print("url:", url)
        if crashed:
            print(f"\n{self.server.server_address} Received PUT request while crashed, ignoring\n")
            return
        
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8')
        print(f"{self.server.server_address} Received PUT request with data: {data}")
        
        # none of the internal endpoints have been found so we assume this is from outside the cluster        
        if (im_leader and url == "/send_to_leader"):
            print(f"leader received put request with data: {data}")
            
            local_log.append(data)
            
            self.send_response(200) # leader received data
            self.end_headers()

            ConnectionHandler(args=(data)).start()
            

        elif url.endswith("append"): # this is an internal endpoint received by followers
            
            # new, still very basic log replication without any checks for mistakes (will not recover, will not find new leader)
            local_log.append(data) # TODO: add log voting here

            # global term, index, new_log

            # if (local_log[-1][1] == term and local_log[-1][2] == index - 1): # if term is the same, and the index is the next one (no missing data)
            #     new_log = data
            self.send_response(200)
            self.end_headers()
        else:
            print("passing request to leader")
            try:
                url = f"http://{leader}/send_to_leader"
                response = requests.put(url, data=data.encode('utf-8'), timeout=5)
                print("finished sending request to leader")
                self.send_response(200)
                self.end_headers()

            except requests.exceptions.Timeout:
                print(f"Request to {leader} timed out >:(")
        
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
        
        # This endpoint commits the current new_log into local_log
        elif url == "/commit":
            global new_log
            local_log.append(new_log)
            # Clear new log 
            new_log = ()    
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