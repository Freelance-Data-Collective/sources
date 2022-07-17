#pipx install personio-py
from personio_py import Personio


class PersonioSource:
    _client_id: str = None
    _client_secret: str = None

    def __init__(self, client_id: str, client_secret: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret

    @property
    def client(self):
        self._client = Personio(client_id=self._client_id, client_secret=self._client_secret)
        return self._client


    def get_employees(self):
        employees = self.client.get_employees()
        for e in employees:
            yield e.to_dict()

    def get_absences(self):
        employees = self._client.get_employees()
        for e in employees:
            absences = self.client.get_absences(e)
            for a in absences:
                ab = a.to_dict()
                ab['employee_id']=e.id_
                yield ab

    def tasks(self):
        tsks = [{'table_name':'employees', 'data':self.get_employees()},
                {'table_name':'absences', 'data': self.get_absences()}
        ]

        return tsks