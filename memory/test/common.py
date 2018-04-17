import requests


class RequestSender:
    @staticmethod
    def post(url, params):
        return requests.post(url, json=params)

    @staticmethod
    def get(url):
        return requests.get(url)


class ToolBoxAPIWrapper:
    @staticmethod
    def query_memory(url, params):
        response = RequestSender.post(url, params)
        return response.status_code, response.json()

    @staticmethod
    def get_memory(url):
        response = RequestSender.get(url)
        return response.status_code, response.json()



