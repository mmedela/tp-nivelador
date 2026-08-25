import socket
import logger
import safe_socket

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                length_bytes = safe_socket.recv_all(client_socket, 4)
                length = int.from_bytes(length_bytes, byteorder="big")
                if length == 0:
                    logger.info(
                        action, 
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount
                    )
                    return
                
                client_message = safe_socket.recv_all(
                    client_socket,
                    length
                )
                message_amount += 1
                safe_socket.send_all(client_socket, length.to_bytes(4, byteorder="big"))
                safe_socket.send_all(client_socket, client_message)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

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
