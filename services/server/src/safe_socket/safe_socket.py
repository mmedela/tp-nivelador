import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buffer = bytearray()
    while len(buffer) < size:
        chunk = socket.recv(size-len(buffer))
        if not chunk:
            continue
        buffer.extend(chunk)
    return bytes(buffer)


def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        total_sent += socket.send(bytes[total_sent:])
    return total_sent
