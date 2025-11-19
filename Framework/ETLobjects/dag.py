import yaml
import re
from enum import Enum, EnumType

from operator import itemgetter
from gitlab.exceptions import GitlabGetError

from Framework.utils.Gitlab_etl_worker import GitlabMetaRepoFactory, NoSuchFileInRepo
from Framework.utils.bigtedi_util import BigTediAPIClient

from typing import List, Union, Dict, Any, overload, Tuple, Optional

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


def enum_to_str_list(object):
    final_list = []
    if isinstance(object, list):
        for i in object:
            if type(i) in [TediGpLoaders, TediMovLoaders, TediDlhLoaders]:
                final_list.append(str(i.value))
            elif isinstance(i, str):
                final_list.append(i)
    elif type(object) == EnumType:
        for i in object:
            final_list.append(str(i.value))
    return final_list


class TediGpLoaders(Enum):
    SCD2 = 'tedi.sinks.greenplum_scd2.GreenplumSCD2'
    SCD1 = 'tedi.sinks.greenplum_scd1.GreenplumSCD1'


class TediMovLoaders(Enum):
    cassandra_loader = 'tedi.sinks.cassandra_loader.CassandraLoader'
    ch_loader = 'tedi.sinks.clickhouse_loader.ClickHouseLoader'
    kafka_loader = 'tedi.sinks.kafka_loader.KafkaLoader'
    oracle_loader = 'tedi.sinks.oracle_loader.OracleLoader'
    pg_loader = 'tedi.sinks.postgres_loader.PostgresLoader'


class TediDlhLoaders(Enum):
    # пока мы не знаем обозначений, предварительный класс
    SCD1 = 'tedi.sinks.spark_scd1.SparkSCD1'
    SCD2 = 'tedi.sinks.spark_scd2.SparkSCD2'


class TediDlhTransforms(Enum):
    spark_sql = 'tedi.transformers.spark_sql.SparkSQL'

LoadersTypes = Union[TediGpLoaders, TediMovLoaders, TediDlhLoaders, List[str]]


class TediGpTransforms(Enum):
    sql = 'tedi.transformers.greenplum_sql.GreenplumSQL'
    gp_fr = 'tedi.transformers.greenplum_fetch_row.GreenplumFetchRow'
    gp_view = 'tedi.transformers.greenplum_view.GreenplumView'
    pandas = 'tedi.transformers.greenplum_pandas.GreenplumPandas'


class TediSpecTransforms(Enum):
    id_tr = 'tedi.transformers.id_translate.IDTranslate'
    vj = 'tedi.transformers.version_join.VersionJoin'
    fp = 'tedi.transformers.field_pulling.FieldPulling'
    rh = 'tedi.transformers.rollup_history.RollupHistory'
    dm = 'tedi.transformers.data_metrics.DataMetrics'
    iterator_for = 'tedi.control_structure.iterator_for.IteratorFor'
    email = 'tedi.sinks.send_email.SendEmailOperator'


class DagNotInRepository(Exception):
    pass


depend_dags_cash = {}


def check_repo_parse(func):
    def wrap_func(*args, **kwargs):
        if isinstance(args[0], TediDag):
            if not args[0].parsed_from_repo:
                args[0].init_repo_files()
            if not args[0].in_repo:
                raise DagNotInRepository("Can't find DAG - '{0}' in branch - '{1}'".format(args[0].name,
                                                                                           args[0].branch))
        return func(*args, **kwargs)
    return wrap_func


class TediDag:
    contour_default='TestVialContour'
    custom_tedi_api_conf=None
    def __init__(self, name, branch='master', use_tedi_api=True, lazy=True, contour=None,
                 custom_config_for_tedi_api=None, strong_branch=False):
        self.name = name
        self.branch = branch
        self.strong_branch = strong_branch
        if 'dw' in self.branch:
            self.branch = self.branch.upper()
        self.repo_path = None

        self.properties = None
        self.meta = None
        self.transforms = []
        self.loader_description = None

        if not custom_config_for_tedi_api:
            custom_config_for_tedi_api = self.custom_tedi_api_conf
        cont = contour if contour else self.contour_default

        if custom_config_for_tedi_api:
            # пример кастомного конфига:
            # from Config.special_configs import BigTediConfig
            #
            # conf = BigTediConfig("DevelopmentContour",
            #                      custom_url="https://dw-48370.vial.k8s-stand-master-1.ds.dwhbigtedi.local")
            # dag = TediDag('dv_some_dag, contour=""DevelopmentContour", custom_config_for_tedi_api=conf)
            self.tedi_api = BigTediAPIClient(cont, config=custom_config_for_tedi_api)
        else:
            self.tedi_api = BigTediAPIClient(cont)

        if use_tedi_api:
            info = self.tedi_api.get_dag_info(self.name)
            self.properties = info.get('properties')
            self.transforms = info.get('tasks', {})
            self.repo_path = info.get('location')
            ui_info = self.tedi_api.get_ui_info(self.name)
            self.loader_description = self.get_loader_description(ui_info)
            try:
                del info['properties']
                del info['tasks']
            except Exception as e:
                pass
            self.meta = info
            self.parsed_from_repo = True
        else:
            self.parsed_from_repo = False

        self.in_repo = True
        if not lazy:
            self.init_repo_files()

    def __repr__(self):
        return f'TediDag: {self.name}'

    def init_repo_files(self):
        repo_api = GitlabMetaRepoFactory(repo='Tedi')
        try:
            files = repo_api.get_files_of_dag_in_repo(dag_name=self.name, branch=self.branch,
                                                      strong_branch=self.strong_branch)
        except GitlabGetError as e:
            self.branch = 'test'
            files = repo_api.get_files_of_dag_in_repo(dag_name=self.name, branch=self.branch,
                                                      strong_branch=self.strong_branch)
        if not files:
            try:
                self.branch = 'test'
                files = repo_api.get_files_of_dag_in_repo(dag_name=self.name, branch=self.branch,
                                                          strong_branch=self.strong_branch)
            except GitlabGetError as e:
                pass

        if files:
            meta_file = ''
            properties_file = ''

            for file in files:
                if file.split('/')[-1] == 'meta.yaml':
                    meta_file = file
                if file.split('/')[-1] == 'properties.yaml':
                    properties_file = file

            if meta_file:
                files.pop(files.index(meta_file))

                try:
                    text = repo_api.get_file_text(meta_file, branch=self.branch)
                except GitlabGetError as e:
                    error_text = str(e)
                    if '404' in error_text:
                        self.branch = 'test'
                        text = repo_api.get_file_text(meta_file, branch=self.branch)
                    else:
                        raise e

                self.meta = yaml.safe_load(text)
                # эти атрибуты есть в базовой модели, которую отдает Tedi API, для стандартизации
                if not self.meta.get('dag_id'):
                    self.meta['dag_id'] = self.name
                if not self.meta.get('location'):
                    location = meta_file.split(self.name + '/')[0]
                    if location:
                        location = location[:-1]
                    self.meta['location'] = location
                tasks = self.meta.get('tasks')
                if tasks:
                    self.transforms = tasks
                del self.meta['tasks']

            if properties_file:
                files.pop(files.index(properties_file))

                text = repo_api.get_file_text(properties_file, branch=self.branch)
                self.properties = yaml.safe_load(text)

            for file in files:
                text = repo_api.get_file_text(file, branch=self.branch)
                if '.sql' in file.split('/')[-1]:
                    index = None
                    for i, transform in enumerate(self.transforms):
                        temp_id = transform.get("task_id")

                        if temp_id and temp_id == file.split('/')[-1][:-4]:
                            index = i
                    if index is not None:
                        self.transforms[index].update({'sql': text})
        else:
            self.in_repo = False

        self.transforms = [self._adjust_task(task) for task in self.transforms]

        self.parsed_from_repo = True

    def get_repo_path(self):
        if self.repo_path is None:
            repo_api = GitlabMetaRepoFactory(repo='Tedi')
            try:
                files = repo_api.get_files_of_dag_in_repo(dag_name=self.name, branch=self.branch)
            except GitlabGetError as e:
                self.branch = 'test'
                files = repo_api.get_files_of_dag_in_repo(dag_name=self.name, branch=self.branch)

            if files:
                self.repo_path = files[0].split(self.name)[0]
            else:
                self.in_repo = False
        return self.repo_path

    @check_repo_parse
    def get_transforms_code(self):
        return self.transforms

    @check_repo_parse
    def get_loaders_types(self) -> Dict[str, List[str]]:
        """Берем по 3 основным линиям: gp, mov, dlh
        возвращаем полный словарь, считаем что загрузчиков произвольное количество
        """
        loaders_by_types = {'gp': [], 'dlh': [], 'mov': []}
        for transform in self.transforms:
            node_type = transform.get('type')
            if node_type:
                if node_type in enum_to_str_list(TediGpLoaders):
                    loaders_by_types['gp'].append(node_type)
                elif node_type in enum_to_str_list(TediDlhLoaders):
                    loaders_by_types['dlh'].append(node_type)
                elif node_type in enum_to_str_list(TediMovLoaders):
                    loaders_by_types['mov'].append(node_type)

        return loaders_by_types

    @check_repo_parse
    def _get_base_loader(self, loader_types: LoadersTypes) -> Optional[Dict[str, str]]:
        """
            Вовзвращает словарь с информацией о лоадере

            Returns
            -------
            loader : Optional[Dict[str, str]]
                Словарь с информацией о лоадере
        """

        loader = None
        loader_types = enum_to_str_list(loader_types)
        # переворачиваем список, т.к. нода с лоадером ожидается последней
        for _, transform in enumerate(reversed(self.transforms)):
            if transform.get('type') in loader_types:
                loader = transform
                break  # принимаем за истину, что в даге только один лоадер
        return loader

    @check_repo_parse
    def _get_kafka_loader(self):
        return self._get_base_loader([TediMovLoaders.kafka_loader.value])

    @check_repo_parse
    def get_kafka_targets(self):
        loader = self._get_kafka_loader()
        loader_params = loader.get('params')
        target_params = {'schema_name': '',
                         'schema_registry_conn_id': '',
                         'schema_version': '',
                         'topic': ''}
        if loader_params:
            target_params['schema_name'] = loader_params.get('schema_name')
            target_params['schema_registry_conn_id'] = loader_params.get('schema_registry_conn_id')
            target_params['schema_version'] = loader_params.get('schema_version')
            target_params['topic'] = loader_params.get('topic')

        return target_params

    @check_repo_parse
    def _get_dlh_loader(self):
        return self._get_base_loader(TediDlhLoaders)

    @check_repo_parse
    def get_dlh_targets(self):
        """Метод для получения таргетов в DLH"""
        try:
            loader = self._get_dlh_loader()
            if not loader:
                target = []
            else:
                target = loader.get('target_entities')
                # В неявном виде разделение парсинга на если лоадер получен из Репы и если из Теди
                if target:
                    if len(target) == 1 and target[0][:2] not in ('vl', 'pr', 'te', '<>'):
                        target = [loader.get('params').get('target')]
                else:
                    target = loader.get('params').get('entity_target')
                    if target:
                        tgts = []
                        for tgt in target.values():
                            if isinstance(tgt, list):
                                tgts.append(tgt[1].replace('<>_', 'test_'))
                            else:
                                tgts.append(tgt.replace('<>_', 'test_'))
                        target = tgts
                    else:
                        target = []
        except TypeError as e:
            target = []

        return target

    def _get_loader_transform(self) -> Optional[Dict[str, str]]:
        """
        Вовзвращает словарь с информацией о лоадере GP

        Returns
        -------
        loader : Optional[Dict[str, str]]
            Словарь с информацией о лоадере
        """

        return self._get_base_loader(TediGpLoaders)

    @check_repo_parse
    def get_target_tables(self) -> List[str]:
        """
        Вовзращает массив таргетов.
        Поддерживает формирование таргетов из репозитория и Теди
        P.S. Принимаем за истину, что таргет у дага один, но чтобы остальной код не сломался - оставляем массив
        ToDo: https://jira.tcsbank.ru/browse/DPQA-1473

        Returns
        -------
        target : List[str]
            Список таргетов
        """
        try:
            loader = self._get_loader_transform()
            if not loader:
                target = []
            else:
                target = loader.get('target_entities')
                # В неявном виде разделение парсинга на если лоадер получен из Репы и если из Теди
                if target:
                    if len(target) == 1 and target[0][:2] not in ('vl', 'pr', 'te', '<>'):
                        target = [loader.get('params').get('target')]
                else:
                    target = loader.get('params').get('entity_target')
                    if target:
                        tgts = []
                        for tgt in target.values():
                            if isinstance(tgt, list):
                                tgts.append(tgt[1].replace('<>_', 'test_'))
                            else:
                                tgts.append(tgt.replace('<>_', 'test_'))
                        target = tgts
                    else:
                        target = []
        except TypeError as e:
            target = []

        return target

    def _get_loader_keys(self, loader):
        """Метод для определени параметров ключей таргетов из параметров лоадера
        : param : loader - объект dict с распарсеными сущностями из repo или API BigTedi
        : return: список строк ключей
        """
        all_keys = []
        params = loader.get('params')
        if params:
            bis_keys = params.get('business_keys')
            if bis_keys:
                # Здесь и ниже возможно излишний код, но универсальный в том плане, что не зависит от того
                # в каком виде возвращаются те или иные ключи - списком или отдельным значением.
                if isinstance(bis_keys, list):
                    all_keys.extend(bis_keys)
                else:
                    all_keys.append(bis_keys)

            keys = params.get('key')
            if keys:
                if isinstance(keys, list):
                    all_keys.extend(keys)
                else:
                    all_keys.append(keys)

            business_dt = params.get('business_dt')
            if business_dt:
                if isinstance(business_dt, list):
                    all_keys.extend(business_dt)
                else:
                    all_keys.append(business_dt)

            load_key = params.get('load_key')
            if load_key:
                if isinstance(load_key, list):
                    all_keys.extend(load_key)
                else:
                    all_keys.append(load_key)

        return all_keys

    @check_repo_parse
    def get_dlh_targets_with_busines_keys_and_loader_type(self) -> Tuple[Dict[str, List[str]], Dict[str, bool]]:
        """
        Возвращает два словаря. Ключи словарей - имя таргетов, значения - список ключей и флаг версионности

        """
        all_targets = self.get_dlh_targets()

        # Используем только первый элемент, т.к. считаем, что таргет один
        target = all_targets[0] if all_targets else None
        target_with_keys = dict()
        target_with_version_type_flg = dict()
        loader = self._get_dlh_loader()

        if loader and target:
            if loader.get('type') == TediDlhLoaders.SCD2.value:
                version_type_flg = True
            else:
                version_type_flg = False

            all_keys = self._get_loader_keys(loader)
            target_with_keys[target] = all_keys
            target_with_version_type_flg[target] = version_type_flg

        return target_with_keys, target_with_version_type_flg

    @check_repo_parse
    def get_targets_with_busines_keys_and_loader_type(self) -> Tuple[Dict[str, List[str]], Dict[str, bool]]:
        """
        Возвращает два словаря. Ключи словарей - имя таргетов, значения - список ключей и флаг версионности

        """
        all_targets = self.get_target_tables()

        # Используем только первый элемент, т.к. считаем, что таргет один
        target = all_targets[0] if all_targets else None
        target_with_keys = dict()
        target_with_version_type_flg = dict()
        loader = self._get_loader_transform()

        if loader and target:
            if loader.get('type') == TediGpLoaders.SCD2.value:
                version_type_flg = True
            else:
                version_type_flg = False

            all_keys = self._get_loader_keys(loader)
            target_with_keys[target] = all_keys
            target_with_version_type_flg[target] = version_type_flg

        return target_with_keys, target_with_version_type_flg


    @check_repo_parse
    def get_source_tables(self):
        sources = []
        for transform in self.transforms:
            source = transform.get('source_entities')
            if source:
                for _ in source:
                    sources.append(_)
        return sources

    def get_last_tasks_logs(self):
        tasks_instances = self.tedi_api.get_dag_tasks_instance(self.name)
        inst_id = tasks_instances.get('dag_run_id')
        tasks_logs = []
        tasks = tasks_instances.get('task_instances')
        try:
            tasks.sort(key=itemgetter('priority_weight'))
        except Exception as e:
            pass
        if inst_id:
            for task in tasks:
                if task['state'] != 'skipped':
                    log = self.tedi_api.get_log_of_task(self.name, inst_id, task['task_id'])
                    tasks_logs.append({task['task_id']: log})

        return tasks_logs

    @overload
    def get_dags_idtr_params(self, convert_to_str: Literal[False]) -> List[Dict[str, Any]]:
        ...

    @overload
    def get_dags_idtr_params(self, convert_to_str: Literal[True]) -> List[str]:
        ...

    def get_dags_idtr_params(self, convert_to_str: bool = False) -> Union[List[Dict[str, Any]], List[str]]:
        """
        Метод для получения параметров id_translate из дага
        :param convert_to_str: влияет на возвращаемое значение. По умолчанию (False) возвращает список словарей.
        Если True, преобразует словари в строки
        :return: список параметров id_translate
        """
        id_translates = []
        for node in self.transforms:
            if node['type'].startswith('tedi.transformers.id_translate'):
                node_id_trs = node['params']['bk_params']
                area = node['params'].get('area')
                if area:
                    for id_tr in node_id_trs:
                        id_tr['area'] = area
                field_rk = node['params'].get('field_rk')
                if field_rk:
                    for id_tr in node_id_trs:
                        id_tr['field_rk'] = field_rk
                id_translates.extend(node_id_trs)

        if not convert_to_str:
            return id_translates

        idtr_strings = []
        for idtr in id_translates:
            idtr_params = ''
            for param in sorted(idtr.keys()):
                idtr_params += param + ':' + str(idtr[param]) + '\n'
            idtr_params += '\n'
            idtr_strings.append(idtr_params)
        return idtr_strings

    @staticmethod
    def get_table_alias_from_code(table: str, code: str) -> List[str]:
        """
        Метод для получения алиаса таблицы в коде
        :param table: таблица в формате test_schema.name, например, test_emart.financial_account
        :param code: sql-код
        :return: список из алиасов таблицы, используемых в коде. Список пуст, если алиасы не найдены
        """
        alias_regexp = re.compile(rf"{table}\s*(as)*\s*(\w*)")
        key_words = {'where', 'inner', 'join', 'left', 'right', 'full', 'cross', 'group', 'using', 'on'}

        return [match.group(2) for match in alias_regexp.finditer(code.lower())
                if match.group(2) not in key_words]

    def get_unloading_fields(self) -> Dict[str, List[str]]:
        """
        Формирует словарь, содержащий в качестве ключа таргет, а в качестве значений массив его полей

        Returns
        -------
        unloading_fields: dict[str: List[str]]
            Словарь, содержащий в качестве ключа таргет, а в качестве значений массив его полей
        """
        unloading_fields = dict()
        types_of_loader = ['tedi.sinks.greenplum_scd1.GreenplumSCD1', 'tedi.sinks.greenplum_scd2.GreenplumSCD2',
                           'tedi.sinks.clickhouse_loader.ClickHouseLoader']
        dag_info = self.tedi_api.get_dag_info(self.name, tasks_only=True)
        for task in dag_info['tasks']:
            if task['type'] in types_of_loader:
                fields = self.tedi_api.get_task_fields(self.name, task['task_id'])
                unloading_fields[task['params']['target']] = fields
        return unloading_fields

    @staticmethod
    def _adjust_task(task_from_repo: dict) -> dict:
        """
        Метод для преобразования ноды в формате репозитория gitlab в ноду в формате ручки BigTEDI
        :param task_from_repo: нода в формате репозитория
        :return: нода в формате ручки /dags/dag_id
        """
        common_params = frozenset({'source', 'source_entities', 'task_id',
                                   'type', 'description', 'target_entities'})
        adjusted_task = dict()
        adjusted_task['params'] = dict()
        for key, value in task_from_repo.items():
            if key == 'entity_source':
                adjusted_task['source_entities'] = list(value.keys())
                adjusted_task['entity_source'] = value
            elif key in common_params:
                adjusted_task[key] = value
            else:
                adjusted_task['params'][key] = value

        if adjusted_task['type'] == 'tedi.transformers.greenplum_sql.GreenplumSQL' \
                and 'source' not in adjusted_task.keys():
            adjusted_task['source'] = []

        if 'sql' in adjusted_task['params'].keys() and 'entity_source' in adjusted_task.keys():
            for meta, phys in adjusted_task['entity_source'].items():
                table_phys_name = phys.replace('<>', 'test')
                adjusted_task['params']['sql'] = re.sub(rf"{{{{ ti\.xcom_pull\(key='{meta}'\) }}}}",
                                                        table_phys_name, adjusted_task['params']['sql'])

        if 'entity_source' in adjusted_task.keys():
            del adjusted_task['entity_source']
        return adjusted_task

    @staticmethod
    def get_loader_description(ui_info):
        loader_description = None
        all_loader_types = {member.value for enum in (TediGpLoaders, TediMovLoaders, TediDlhLoaders) for member in enum}
        for node in ui_info.get('nodes', []):
            if node.get('data') and node.get('data').get('values') \
                and node.get('data').get('values').get('node_type') in all_loader_types:
                loader_description = node.get('data').get('values').get('description')
        return loader_description
