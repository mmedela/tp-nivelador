import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buffer = bytearray()
    while len(buffer) < size:
        chunk = socket.recv(size-len(buffer))
        if not chunk:
            raise OSError("Connection closed before recieving full payload")
        buffer.extend(chunk)
    return bytes(buffer)


def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])
        if sent == 0:
            raise OSError("Connection closed before sending full payload")
        total_sent += sent
        
    return total_sent
