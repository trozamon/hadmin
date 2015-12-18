""" Mocks for testing """


class ResponseMock:

    def __init__(self, content, status):
        self.content = content
        self.status = status

    def read(self):
        return self.content


class JMXConnectionMock:

    def request(self, req_type, path):
        if req_type == 'GET' and path == '/jmx':
            self.requested = True

    def getresponse(self):
        if self.requested:
            with open('data/datanode.jmx.json') as f:
                return ResponseMock(f.read(), 200)

        return ResponseMock('', 404)


class RESTConnectionMock:

    def request(self, req_type, path):
        if req_type == 'GET' and path == '/ws/v1/node':
            self.requested = True

    def getresponse(self):
        if self.requested:
            with open('data/nodemanager.rest.json') as f:
                return ResponseMock(f.read(), 200)

        return ResponseMock('', 404)
