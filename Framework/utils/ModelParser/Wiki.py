import enum
import urllib3

import requests
import bs4
from bs4 import BeautifulSoup

from Config.special_configs import GlobalConfig
from typing import List, Optional, Match, Dict, Set, Tuple

from atlassian import Confluence

import os
import time
from Framework.utils.jira import JiraApi
import re
from dwh_common_etl_qa.helpers.requests.adapters import TimeoutHTTPAdapter


def run_with_exception(func):
    def wrapper(*args, **kwargs):
        status_code, res, exc = func(*args, **kwargs)
        if exc:
            raise WikiClient.WikiException('Error when running: {0}'.format(exc))
        if status_code != 200:
            raise WikiClient.WikiException('API error. Confluence return code - {0}'.format(str(status_code)))
        return res
    return wrapper


class WikiTableHeaders(enum.Enum):
    NAME = 'имя'
    TASK = ['задача в jira', 'jira task']
    SECRET = 'чд'


class WikiClient:
    RETRIES_REQUESTS = 5

    class WikiException(Exception):
        pass

    def __init__(self):
        conf = GlobalConfig()
        self.ad_user = conf.etl_meta_user
        self.ad_passw = conf.etl_meta_pa

        self.base_url_model = 'https://wiki.tcsbank.ru/'
        self._adapter = TimeoutHTTPAdapter(max_retries=urllib3.Retry(
            total=self.RETRIES_REQUESTS,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504]
        ))

    def get_confluence_auth(self) -> Confluence:
        session = requests.session()
        session.mount('http://', self._adapter)
        session.mount('https://', self._adapter)

        confluence = Confluence(
            url=self.base_url_model,
            username=self.ad_user,
            password=self.ad_passw,
            session=session
        )
        return confluence

    def get_wiki_page(self, confluence, url):
        try:
            page = confluence.get_page_by_title(
                space='DW',
                title=url,
                expand='body.storage',
            )
            return page
        except Exception as err:
            raise WikiClient.WikiException(f'Error by getting page from Wiki - {repr(err)}')

    def full_web_url(self, uri):
        return self.base_url_model + 'display/DW/' + uri

    def get_potential_wiki_urls(self, full_table_name):
        area = full_table_name.split(' ')[0]
        table = full_table_name[len(area) + 1:].replace(' ', '_')
        alt_url = 'DWH'+"{0}.{1}".format(area, table)
        if area == 'DDS':
            url = alt_url
        elif area == 'DDSD':
            url = 'DDS_DIC'+".{0}".format(table)
        elif area == 'DWH':
            table = full_table_name[len('DWH REP') + 1:].replace(' ', '_')
            url = 'DWH_REP' + ".{0}".format(table)
        else:
            url = "{0}.{1}".format(area, table)
        return url, alt_url

    def get_potential_dags_wiki_urls(self, full_table_name):
        table = full_table_name.split('<>_')[1]
        if table.split('.')[0] == 'datavault':
            table = 'dv' + '.' + table.split('.')[1]
        if table.split('.')[0] == 'integration_mart':
            table = 'imart' + '.' + table.split('.')[1]
        if table.split('.')[0] == 'ddsd':
            table = 'dds_dic' + '.' + table.split('.')[1]
        url = "{0}".format(table)
        alt_url = "{0}".format('dwh' + table)

        return url, alt_url

    def auth_wiki(self):
        try:
            s = requests.session()
            s.mount('http://', self._adapter)
            s.mount('https://', self._adapter)
            r = s.get('https://wiki.tcsbank.ru', timeout=10)
            if r.status_code == 200:
                if BeautifulSoup(r.content, 'html.parser').title.text.startswith('Log'):
                    s.post('https://wiki.tcsbank.ru/dologin.action', {
                        'os_username': self.ad_user,
                        'os_password': self.ad_passw,
                    })
            return s
        except Exception as err:
            raise WikiClient.WikiException(f'Error to connect Wiki - {repr(err)}')

    @staticmethod
    def r_pars(source) -> str:
        try:
            while True:
                if hasattr(source, 'contents'):
                    source = source.text
                else:
                    break
        except Exception as e:
            pass
        return str(source)

    def _parse_lt_in_table(self, table, field_name_index, table_header, index_to_check=1):
        table_rows = table.find_all('tr')
        lt = []
        for tr in table_rows[index_to_check:]:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            if len(row) == len(table_header) and not td[field_name_index].find('s'):
                lt.append(row)

        return table_header, lt

    def _get_all_wiki_columns_for_2_tags(self, table_head, table_rows) -> tuple:
        table_header = self._get_table_header(table_head)
        return self._parse_lt_in_table(table_rows,
                                       table_header.index(WikiTableHeaders.NAME.value),
                                       table_header,
                                       index_to_check=0)

    def get_all_wiki_columns(self, urls) -> tuple:
        main_table, rows_table = self._get_main_table(urls)
        if rows_table:
            return self._get_all_wiki_columns_for_2_tags(main_table, rows_table)
        else:
            table_header = self._get_table_header(main_table)
            field_name_index = table_header.index(WikiTableHeaders.NAME.value)
            return self._parse_lt_in_table(main_table, field_name_index, table_header)

    def get_pd_columns(self, urls) -> Optional[Set[str]]:
        pd_columns = set()
        table_names, table_values = self.get_all_wiki_columns(urls)
        for row in table_values:
            sensetive = False
            value = None
            for step in zip(table_names, row):
                if step[0].lower() == WikiTableHeaders.SECRET.value and step[1] != '':
                    sensetive = True
                if step[0].lower() == WikiTableHeaders.NAME.value:
                    value = step[1].strip()
            if sensetive:
                pd_columns.add(value)
        return pd_columns

    def get_wiki_columns(self, urls) -> Optional[Set[str]]:
        table_names, table_values = self.get_all_wiki_columns(urls)
        cols = set()
        for row in table_values:
            for step in zip(table_names, row):
                if step[0].lower() == WikiTableHeaders.NAME.value:
                    cols.add(step[1])
        return cols

    def get_column_and_task_info(self, urls: List[str], columns: Set[str]) -> dict:
        """Получение информации о колонках таблицы (перечеркнуты, последняя задача в Jira)
        :param urls: потенциальлные урлы до страницы
        :param columns: список колонок, о которых нужна информация"""
        main_table, _ = self._get_main_table(urls)
        table_header = self._get_table_header(main_table)
        field_name_index = table_header.index(WikiTableHeaders.NAME.value)

        for task in WikiTableHeaders.TASK.value:
            try:
                jira_task_index = table_header.index(task)
            except ValueError:
                continue

        table_rows = main_table.find_all('tr')
        jira = JiraApi.get_jira()
        jira_task_pattern = re.compile(r'((?:DW|DWD)-\d{1,6})')
        column_pattern = re.compile(r'(\w*)')
        cols = set(columns)

        field_task_info = dict()

        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            if len(row) == len(table_header):
                col_from_row = column_pattern.search(row[field_name_index])
                if col_from_row:
                    col_from_row = col_from_row.group(1).lower()
                    for col in cols:
                        if col == col_from_row:
                            jira_task = jira_task_pattern.findall(row[jira_task_index])
                            status = None
                            if jira_task:
                                jira_task = jira_task[-1]
                                try:
                                    issue = jira.issue(jira_task)
                                    status = issue['fields']['status']['name']
                                except (KeyError, TypeError):
                                    pass

                            is_strike = True if td[field_name_index].find('s') else False

                            field_task_info[col] = {'is_strike': is_strike, 'task': jira_task, 'task_status': status}

                            cols.remove(col)
                            break
            if len(cols) == 0:
                break

        return field_task_info

    def _get_btfs_page(self, urls: List[str]):
        confluence = self.get_confluence_auth()
        page = self.get_wiki_page(confluence, urls[0])
        if not page:
            page = self.get_wiki_page(confluence, urls[1])

        if not page:
            raise WikiClient.WikiException('Страница на вики не найдена или не доступна!')

        soup = BeautifulSoup(page['body']['storage']['value'], 'html.parser')
        return soup

    def _get_prototype(self, urls: List[str]):
        prot_code = ''
        soup = self._get_btfs_page(urls)
        codes = soup.find_all(text=lambda tag: isinstance(tag, bs4.CData))
        if codes:
            prot_code = str(codes[-1].string)

        return prot_code

    def get_prototype(self, urls):
        return self._get_prototype(urls)

    def _get_main_table(self, urls: List[str]):
        """Получение информации о колонках таблицы (перечеркнуты, последняя задача в Jira)
        :param urls: потенциальные урлы до страницы
        :return: таблица в формате bs4.element.Tag, содержащая колонки"""
        soup = self._get_btfs_page(urls)
        all_tables = soup.find_all('table')
        table_header = None
        table_body = None

        for table in all_tables:
            if table.text.lower().count('ключ') > 0:
                # сделана такая фильтрация, т.к. в table.contents для определенных объектов
                # в качестве отдельных элементов списка присутствуют символы перевода строк
                # из-за этого вместо пары (thead и tbody) возвращается (thead и \n)
                table_contents = list(filter(lambda x: (x != '\n')
                                                       and (x.name.lower() != 'colgroup'),
                                             table.contents))

                if len(table_contents) > 1:
                    table_header, table_body = table_contents
                else:
                    table_header = table_contents[0]
                break

        return table_header, table_body

    @staticmethod
    def _get_table_header(table):
        first_str = table.find('tr')
        table_header = [th.text for th in first_str.find_all('th')]
        if table_header:
            return WikiClient._adjust_columns(table_header)

        table_header = [th.text.strip() for th in first_str.find_all('td')]
        if table_header:
            return WikiClient._adjust_columns(table_header)
        else:
            raise WikiClient.WikiException("Заголовки таблицы не заключены в тэги <th> или <td>")

    @staticmethod
    def _adjust_columns(columns: List[str]) -> List[str]:
        """Удаление лишних символов из колонок, приведение к нижнему регистру"""
        return [column.strip().lower().replace('\xa0', '') for column in columns]
