import os
import socket
import threading
import time
import logger
from lottery.lottery import Lottery
import protocol
import signal

DEFAULT_GRACE_TIME = 4.0 
class Server:


    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lottery = Lottery(storage_path=os.environ.get("STORAGE_PATH", "/tmp/bets.csv"))

        self.clien_threads = []
        self.shutting_down = threading.Event()
        self.client_sockets = set()
        self.client_sockets_lock = threading.Lock()

        self.lottery_lock = threading.Lock()
        self.threads_lock = threading.Lock()

        self.agency_quorum_min = agency_quorum_min
        self.finished_agencies = set()
        self.quorum_condition = threading.Condition()


    def _handle_sigterm(self, sugnum, frame):
        self.shutting_down.set()

        with self.quorum_condition:
            self.quorum_condition.notify_all()

        try: self.server_socket.close()
        except OSError: pass

        with self.client_sockets_lock:
            for client_socket in list(self.client_sockets):
                try: client_socket.shutdown(socket.SHUT_RDWR)
                except OSError: pass
                try: client_socket.close()
                except OSError: pass


    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        with self.client_sockets_lock:
            self.client_sockets.add(client_socket)
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
                while len(self.finished_agencies) < self.agency_quorum_min and not self.shutting_down.is_set():
                    self.quorum_condition.wait()
                if self.shutting_down.is_set():
                    return

            with self.lottery_lock:
                winners = [b for b in self.lottery.load_bets()
                        if b.agency_id == agency_id and self.lottery.has_won(b)]
            for w in winners:
                protocol.send_message(client_socket, protocol.WINNER, protocol.serialize_winner(w))
            protocol.send_message(client_socket, protocol.FINISH)
            logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
        except Exception as e:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
            return
        finally:
            with self.client_sockets_lock:
                self.client_sockets.discard(client_socket)
            try: client_socket.close()
            except OSError: pass

    def run(self):
        action = "accept-connection"
        with self.server_socket:
            self.server_socket.bind((self.server_host, self.server_port))
            self.server_socket.listen()
            signal.signal(signal.SIGTERM, self._handle_sigterm)
            while not self.shutting_down.is_set():
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = self.server_socket.accept()
                except OSError:
                    if self.shutting_down.is_set():
                        logger.info("Exting gracefully", logger.LogResult.success)
                        break
                    raise
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                client_thread = threading.Thread(target=self._handle_client, args=(client_socket,), daemon=False)
                with self.threads_lock:
                    self.clien_threads.append(client_thread)
                client_thread.start()

            grace_time = os.environ.get("GRACE_TIME", DEFAULT_GRACE_TIME)
            with self.threads_lock:
                for client_thread in self.clien_threads:
                    start = time.monotonic()
                    client_thread.join(timeout=max(0,0, grace_time))
                    grace_time -= time.monotonic() - start
                    if grace_time <= 0: break
