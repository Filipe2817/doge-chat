import socket
import json
import struct

class KVClient:
    def __init__(self, host='localhost', port=8000):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def close(self):
        if self.socket:
            self.socket.close()
            print("Connection closed")
    
    def send_message(self, message):
        if not self.socket:
            print("Not connected")
            return None
        
        json_data = json.dumps(message).encode('utf-8')
        # Add 4-byte length prefix (packet, 4)
        length_prefix = struct.pack(">I", len(json_data))
        
        try:
            self.socket.sendall(length_prefix + json_data)
            
            length_data = self.socket.recv(4)
            if not length_data:
                return None
                
            msg_length = struct.unpack(">I", length_data)[0]
            
            chunks = []
            bytes_received = 0
            while bytes_received < msg_length:
                chunk = self.socket.recv(min(4096, msg_length - bytes_received))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_received += len(chunk)
            
            response_data = b''.join(chunks)
            return json.loads(response_data)
        except Exception as e:
            print(f"Error in communication: {e}")
            return None
    
    def get(self, key):
        message = {
            "type": "get",
            "data": {
                "key": key
            }
        }
        response = self.send_message(message)
        return response

    def set(self, key, value):
        message = {
            "type": "set",
            "data": {
                "key": key,
                "value": value
            }
        }
        response = self.send_message(message)
        return response
    
    def simulate_requests(self):
        requests = [
            ("topic1", "192.168.1.1"), # 3FBD9CD577B36CD03D3C84B7ADF8710DC2446896
            ("topic2", "192.168.1.2"), # 490B6E519F0E2BF8CB3B848FA7FD093D08AD0481
            ("topic3", "192.168.1.3"), # DBB079DCF8579957E4B78DFBB871FC49F1503854
            ("topic4", "192.168.1.4"), # 25509D2F0F313F06D42D9D66145C9EA22319068F
            ("topic5", "192.168.1.5"), # 8A125803375E452EEAE3B15E265AECF98BA28876
            ("topic6", "192.168.1.6"), # 4D5A852C7AB5D4213CFC96DBAB9FA44B71AD6B68
            ("topic7", "192.168.1.7"), # DC8B0C37669A50E589D37FA36EF63AE641BEB5CA
            ("topic8", "192.168.1.8"), # 3378D0E2BAD654FCF9D1000E734692E241140029
            ####
            ("topic3", "192.168.1.30"),
            ("topic1", "192.168.1.10"),
            ("topic2", "192.168.1.20"),
            ("topic4", "192.168.1.40"),
            ("topic7", "192.168.1.70")
        ]

        for key, value in requests:
            self.set(key, value)

def main():        
    client = KVClient()
    
    if not client.connect():
        return
        
    try:
        while True:
            command = input("\nEnter command (get/set/sim/exit): ").strip().lower()
            
            if command == "exit":
                break

            elif command == "get":
                key = input("Enter key: ").strip()
                response = client.get(key)
                print(f"Response: {response}")

            elif command == "set":
                key = input("Enter key: ").strip()
                value = input("Enter value: ").strip()
                response = client.set(key, value)
                print(f"Response: {response}")

            elif command == "sim":
                client.simulate_requests()
                print("Requests simulated.")

            else:
                print("Unknown command")
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.close()

if __name__ == "__main__":
    main()
