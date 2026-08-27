import os
import socket
import threading
import logger
from lottery.lottery import Lottery
import protocol

class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path=os.environ.get("STORAGE_PATH", "/tmp/bets.csv"))

        self.lottery_lock = threading.Lock()

        self.agency_quorum_min = agency_quorum_min
        self.finished_agencies = set()
        self.quorum_condition = threading.Condition()

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            agency_id = None
            while True:
                tag, data = protocol.recv_message(client_socket)
                if tag == protocol.AGENCY:
                    agency_id = int.from_bytes(data, "big")
                elif tag == protocol.BATCH:
                    try:
                        batch_bets = protocol.deserialize_bets(agency_id, data)
                        with self.lottery_lock:
                            self.lottery.store_bets(batch_bets)
                        message_amount += len(batch_bets)
                        protocol.send_message(client_socket, protocol.BATCH_ACK, protocol.serialize_batch_ack(True))
                    except Exception:
                        logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
                        protocol.send_message(client_socket, protocol.BATCH_ACK, protocol.serialize_batch_ack(False))
                elif tag == protocol.FINISH:
                    break

            with self.quorum_condition:
                self.finished_agencies.add(agency_id)
                self.quorum_condition.notify_all()
                while len(self.finished_agencies) < self.agency_quorum_min:
                    self.quorum_condition.wait()

            with self.lottery_lock:
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

                threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
