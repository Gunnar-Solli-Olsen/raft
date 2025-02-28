import http.server
import socketserver
import requests
import random
import threading
import time
import sys
import os
from urllib.parse import urlparse

try:
    output_id, scenario, nodes_list = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
    nodes_list = [node for node in nodes_list]  
except ValueError:
    print("Usage: log-client.py <output_id> <scenario> <node1> <node2> ... <nodeN>")
    sys.exit(1)

lorem_ipsum = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua".split()

def put_log_entries(sc):
    warnings = 0

    for i in range(sc.total_entries):
        target_node = random.choice(nodes_list)
        word = random.choice(lorem_ipsum) + str(random.randrange(10000000))
        word_bytes = word.encode('utf-8')
        success = False
        try:
            print(f"Sending log entry to {target_node}: {word}")
            requests.put(f'http://{target_node}', data=word_bytes, timeout=5)
            sc.local_entries.append(word)
            success = True
        except requests.exceptions.Timeout:
            print(f"Request to {target_node} timed out")
            warnings += 1
        except Exception as e:
            print(f"Failed to send PUT request to {target_node}: {e}")
        
        with sc.crashed_lock:
            if success and target_node in sc.crashed_nodes:
                print(f"Warning: Successfully PUT entry to {target_node}, but it was expected to be crashed")
                warnings += 1
            if not success and target_node not in sc.crashed_nodes:
                print(f"Warning: Failed to PUT entry to {target_node}, but it was not expected to be crashed")
                warnings += 1
        
        time.sleep(random.randint(sc.log_interval[0], sc.log_interval[1]))
    
    print("Finished sending log entries, exiting in 10 seconds...")
    time.sleep(10)
    for node in nodes_list:
        requests.post(f'http://{node}/exit')
    print(f"Total successful entries: {len(sc.local_entries)}")
    print(f"Total warnings: {warnings}")

    with open(f"output/{output_id}-client.csv", 'w') as f:
        for entry in sc.local_entries:
            f.write(f"{entry}\n")
    print(f"Exiting. Output ID=\n{output_id}")
    os._exit(0)

def simulate_crash_and_recovery(sc):
    while True:
        target_node = random.choice(nodes_list)
        with sc.crashed_lock:
            sc.crashed_nodes.append(target_node)
        try:
            requests.post(f'http://{target_node}/crash')
            print(f"Simulated crash for {target_node}")
        except Exception as e:
            print(f"Failed to simulate crash for {target_node}: {e}")
        
        time.sleep(random.randint(sc.crashed_time[0], sc.crashed_time[1]))
        
        try:
            requests.post(f'http://{target_node}/recover')
            with sc.crashed_lock:
                sc.crashed_nodes.remove(target_node)
            print(f"Simulated recovery for {target_node}")
        except Exception as e:
            print(f"Failed to simulate recovery for {target_node}: {e}")

        time.sleep(random.randint(sc.crash_interval[0], sc.crash_interval[1]))

class Scenario:
    def __init__(self, scenario_id):
        self.crashed_lock = threading.Lock()
        self.local_entries = []
        self.crashed_nodes = []

        if scenario_id == 0:
            self.log_interval = [2, 3]
            self.crash_interval = [1, 2]
            self.crashed_time = [5, 15]
            self.total_entries = 20
            self.simultaneous_crashes = 0

        if scenario_id == 1:
            self.log_interval = [2, 3]
            self.crash_interval = [5, 7]
            self.crashed_time = [5, 15]
            self.total_entries = 20
            self.simultaneous_crashes = 1
        
        if scenario_id == 2:
            self.log_interval = [1, 2]
            self.crash_interval = [1, 2]
            self.crashed_time = [3, 4]
            self.total_entries = 100
            self.simultaneous_crashes = 1

        if scenario_id == 3:
            self.log_interval = [2, 3]
            self.crash_interval = [5, 7]
            self.crashed_time = [5, 15]
            self.total_entries = 100
            self.simultaneous_crashes = 2

        if scenario_id == 4:
            self.log_interval = [2, 3]
            self.crash_interval = [5, 7]
            self.crashed_time = [5, 15]
            self.total_entries = 100
            self.simultaneous_crashes = (len(nodes_list) - 1) // 2.0

if __name__ == "__main__":
    sc = Scenario(scenario)

    threading.Thread(target=put_log_entries, args=[sc]).start()
    for i in range(int(sc.simultaneous_crashes)):
        threading.Thread(target=simulate_crash_and_recovery, args=[sc]).start()
