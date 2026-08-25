import os
import socket
import logger
from lottery.lottery import Lottery
import safe_socket
import protocol

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=os.environ.get("STORAGE_PATH", "/tmp/bets.csv")) 

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id, bets = None, []
            while True:
                tag, data = protocol.recv_message(client_socket)
                if tag == protocol.AGENCY:
                    agency_id = int.from_bytes(data, "big")
                elif tag == protocol.BET:
                    bets.append(protocol.deserialize_bet(agency_id, data))
                    message_amount += 1
                elif tag == protocol.FINISH:
                    break
            self.lottery.store_bets(bets)
            winners = [b for b in self.lottery.load_bets()
                    if b.agency_id == agency_id and self.lottery.has_won(b)]
            for w in winners:
                protocol.send_message(client_socket, protocol.WINNER, protocol.serialize_winner(w))
            protocol.send_message(client_socket, protocol.FINISH)
            logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
        except Exception as e:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
            raise e
        finally:
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
