import sys
import glob

def parse_files(files):
    contents = []
    for file in files:
        with open(file, 'r') as f:
            contents.append(f.read().strip().split('\n'))
    return contents

def check_log_consistency(client_log, server_logs, server_log_filenames):
    success = True

    client_log_set = set(client_log)

    total_server_set = set()

    for log in server_logs:
        for entry in log:
            total_server_set.add(entry)

    if len(client_log_set - total_server_set) > 0:
        success = False
        print(f"Error: Some entries sent by client are missing: {client_log_set - total_server_set}")

    for i in range(len(server_logs)):
        if set(server_logs[i]) != client_log_set:
            success = False
            print(f"Error: A server log does not contain all entries sent by the client: {server_log_filenames[i]}")

    if server_logs.count(server_logs[0]) != len(server_logs):
        success = False
        print("Error: Server logs are not identical to each other")

    return success

def main(file_id):
    client_files = glob.glob(f'output/*{file_id}*client*')
    server_files = glob.glob(f'output/*{file_id}*server*')

    print(f"Testing input files: {client_files} {server_files}")
    if (len(client_files) > 1):
        print("Warning! More than one client log included. Only the first one will be used.")

    client_logs = parse_files(client_files)
    server_logs = parse_files(server_files)

    if check_log_consistency(client_logs[0], server_logs, server_files):
        print("Success")
    else:
        print("Failure")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <file_id>")
        sys.exit(1)
    
    file_id = sys.argv[1]
    main(file_id)


