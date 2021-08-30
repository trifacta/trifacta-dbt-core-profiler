import requests

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/83.0.4103.97 Safari/537.36",
}


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r


class TrifactaRequest:
    def __init__(self, config):
        self.config = config

    def get_endpoint(self) -> str:
        return self.config["endpoint"]

    def get_auth(self) -> BearerAuth:
        return BearerAuth(self.config["token"])

    def raw_get(self, url: str, params=None) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.get(endpoint + url, params, auth=auth, headers=headers)
        return r

    def get(self, url: str, params=None) -> requests.request:
        r = self.raw_get(url, params)
        check_success(r)
        return r

    def post(self, url: str, data=None, json=None, files=None, headers=None) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.post(
            endpoint + url, data=data, json=json, files=files, headers=headers, auth=auth
        )
        check_success(r)
        return r

    def delete(self, url: str, headers=None) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.delete(endpoint + url, headers=headers, auth=auth)
        check_success(r)
        return r

    def put(self, url: str, data=None, json=None) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.put(endpoint + url, data=data, json=json, headers=headers, auth=auth)
        check_success(r)
        return r

    def patch(self, url: str, data=None, json=None) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.patch(endpoint + url, data=data, json=json, headers=headers, auth=auth)
        check_success(r)
        return r

    def upload(self, url, filename, file) -> requests.request:
        auth, endpoint = self.get_auth(), self.get_endpoint()
        r = requests.post(endpoint + url, files={"file": (filename, file)}, auth=auth)
        check_success(r)
        return r


def check_success(r: requests.request):
    if r.status_code >= 400:
        try:
            error = r.json()
        except:
            error = r.text
        raise Exception("An error occurred while executing the request", error)


class TrifactaEndpoint:
    def __init__(self, method, url, req={}):
        self.method = method
        self.url = url
        self.req = req

    def invoke(self, tfr: TrifactaRequest):
        if self.method == 'GET':
            return tfr.get(self.url)
        elif self.method == 'POST':
            return tfr.post(self.url, json=self.req)
        elif self.method == 'DELETE':
            return tfr.delete(self.url, json=self.req)
        elif self.method == 'PUT':
            return tfr.put(self.url, json=self.req)
        raise Exception("Unrecognized method: " + self.method)

