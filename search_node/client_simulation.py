import socket
import json
import struct

class KVClient:
    def __init__(self, host='localhost', port=1234):
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
            "client_type": "client",
            "data": {
                "key": key,
                "value": value
            }
        }
        response = self.send_message(message)
        return response

def main():        
    client = KVClient()
    
    if not client.connect():
        return
        
    try:
        while True:
            command = input("\nEnter command (get/set/exit): ").strip().lower()
            
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
                
            else:
                print("Unknown command")
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.close()

if __name__ == "__main__":
    main()
