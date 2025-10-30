import urllib3
import requests
import warnings
import json
from typing import List, Tuple, Dict, Any, Union

from Config.special_configs import MoebiusConfig
from Framework.exceptions import MoebiusApiException
from Framework.utils.base_util import BaseUtil
from dwh_common_etl_qa.helpers.requests.adapters import TimeoutHTTPAdapter


class BaseMoebiusClient(BaseUtil):
    RETRIES_REQUESTS = 5

    def __init__(self, logger=None):
        self._base_sub_url = '/api/v1'
        self._session = requests.session()
        adapter = TimeoutHTTPAdapter(max_retries=urllib3.Retry(
            total=self.RETRIES_REQUESTS,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504]
        ))
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        self._session.trust_env = False
        self._config = MoebiusConfig()
        self._logger = logger
        self.headers = {}
        warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    def get_main_url(self):
        return ''

    def __error_detection(self, resp_data, url):
        err_fl = False
        err_mes = ""
        resp = {}

        if resp_data.status_code == 200:
            if resp_data.text.startswith('{') or resp_data.text.startswith('['):
                resp = json.loads(resp_data.text)
            else:
                resp = resp_data.text

        else:
            err_fl = True
            err_mes = f"ERROR: Can't execute a GET request to Admin Moebius API. Status code is: {resp_data.status_code}." \
                      f"\nUrl: {url}"

        return err_fl, err_mes, resp

    def __execute_get_request(self, url: str) -> Tuple[bool, str, Union[List[Any], Dict[Any, Any], str]]:
        """Метод для выполнения GET запроса .
        :param url: строка с урлом запроса
        :return: кортеж из признака завершения с ошибкой, текста ошибки и словаря с содержимым ответа
        """
        request_url = self.get_main_url() + self._base_sub_url + url

        try:
            resp_data = self._session.get(url=request_url, headers=self.headers, verify=False)
        except Exception as err:
            raise MoebiusApiException(f'Error to connect Moebius API - {repr(err)}')

        return self.__error_detection(resp_data, url)

    def _base_get_request(self, url: str):
        err, err_mes, resp = self.__execute_get_request(url=url)

        if err:
            self.log(err_mes)

        return resp


class MoebiusAdminClient(BaseMoebiusClient):
    def get_main_url(self):
        return self._config.admin_main_url

    def get_prod_url(self):
        return self._config.admin_prod_url

    def get_node_children(self, node_name, recursive='false') -> dict:
        node_children_url = f'/nodes/tedi_dag/{node_name}/children?recursive_flg={recursive}'

        return self._base_get_request(node_children_url)

    def get_packs(self, dag) -> dict:
        packs_url = f'/packs/?job_id={dag}'

        return self._base_get_request(packs_url)


class MoebiusCronClient(BaseMoebiusClient):
    def get_main_url(self):
        return self._config.cron_main_url

    def get_scheduled_events(self) -> dict:
        scheduled_events_url = '/scheduled_events/'

        return self._base_get_request(scheduled_events_url)


class MoebiusAdminSwaggerClient(BaseMoebiusClient):
    def get_main_url(self):
        return self._config.admin_swagger_url

    def get_node(self, dag):
        node_url = f'/nodes/prod_airflow_tedi_flow/urn:task:airflow_tedi:{dag}?render_flg=true'

        return self._base_get_request(node_url)
