import safe_socket
from lottery.bet import Bet

AGENCY, BET, FINISH, WINNER = 1, 2, 3, 4

def send_message(sock, tag, data=b""):
    payload = bytes([tag]) + data
    safe_socket.send_all(sock, len(payload).to_bytes(4, "big") + payload)

def recv_message(sock):
    length = int.from_bytes(safe_socket.recv_all(sock, 4), "big")
    payload = safe_socket.recv_all(sock, length)
    return payload[0], payload[1:]

def deserialize_bet(agency_id, data):
    first, last, doc, birth, num = data.decode().split(",")
    return Bet(agency_id, first, last, int(doc), birth, int(num))

def serialize_winner(bet):
    return f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}".encode()