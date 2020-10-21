import time
import socket
import bisect

class ClientError(Exception):
    pass


class Client:
    """Класс для получения метрики с сервера"""

    def __init__(self, host, port, timeout=None):
        self._host = host
        self._port = port
        self._timeout = timeout

        try:
            self.sock = socket.create_connection((self._host, self._port), self._timeout)
        except socket.error as er:
            raise ClientError("Can't create connection", er)

    def put(self, key, value, timestamp=None):
        timestamp = str(timestamp or int(time.time()))

        try:
            self.sock.sendall(f"put {key} {value} {timestamp}\n".encode())
        except socket.error as er:
            raise ClientError("Error sending data", er)
        data = self.sock.recv(1024).decode('utf-8')
        if 'ok\n\n' in data:
            return
        else:
            raise ClientError("Invalid put request")

    def get(self, key):
        try:
            self.sock.sendall(f"get {key}\n".encode())
        except socket.error as er:
            raise ClientError("Error sending data: ", er)

        data = {}
        raw_data = self.sock.recv(1024).decode('utf-8')
        status, payload = raw_data.split('\n', 1)
        payload = payload.strip()

        if status != 'ok':
            raise ClientError("Invalid get request")
        if payload == '':
            return data

        try:
            for row in payload.splitlines():
                key, value, timestamp = row.split()
                if key not in data:
                    data[key] = []
                bisect.insort(data[key], ((int(timestamp), float(value))))

        except Exception as err:
            raise ClientError('Server returns invalid data', err)

        return data
