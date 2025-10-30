import os
from collections import OrderedDict
import random
import string
import zipfile
from typing import List, Set, Union, Dict, Callable, Optional, Tuple
import xml.etree.ElementTree as etree
import json
from re import findall as re_find
from enum import Enum

from Config import enviroment, user, pack_path, names
from Config.special_configs import GlobalConfig
from Config.feature_store import FeaturesTogles

from common.unix_cmd import SystemCommand
from Framework.special_expansion.special_functions import catch_problem
from Framework.ETLobjects.package import Package, PackageChimeraAdapter
from Framework.utils.package_utils import PackageUtils
from Framework.utils import SASNameTransfer
from Framework.utils.ddl_compare import CompareDdl
from Framework.utils.parse_deploy_and_check import ParseDeployAndCheck
from Framework.utils.consul_util import ConsulAPIClient
from Framework.utils.chimera_util import ChimeraApi
from Framework.ETLobjects.chimera_files import ChimeraCollector
from helpers.parsers import log_parser
from helpers.parsers import SasScriptsParser, ChimeraSqlScriptParser
from Framework.ETLobjects.job import JobDetail
from Framework.ETLobjects.dag import TediDag, DagNotInRepository
from Framework.ETLobjects.table import TableFactory, get_prefix, TableDetail, DlhTable
from Framework.ETLobjects.package import PackageObject
from Framework.utils.cut2_util import Cut2Util
from Framework.ETLobjects.cut2_sttings import Cut2Settings
from common.postgresql import GreenPlumSQL
from Framework.utils.dlh.sparksql import SparkSQL

from dwh_metadata_extractor import MgClient
from Framework.utils.mg_v2_util import MgReplicaGpHelper
from helpers.depend_dict_differ import DepDictDiffer
from helpers.execution_timer import execution_timer
from Framework.utils.parse_deploy_and_check import NoDeploy, CantGetDeployFromRepository
from helpers.common import set_prefix_in_schema


class UrnPrefixes(Enum):
    UrnGpPrefix = 'urn:ph:table:dwh:greenplum:'
    UrnDlhPrefix = 'urn:ph:table:dwh:dlh:'


class BasePackage:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.type = ''
        self._pack = None
        self.sas_scripts = None
        self.cut2_settings = None
        self.chimera_sql_scripts = None
        self.chimera_dlh_sql_scripts = None

        consul = ConsulAPIClient()
        features_settings = consul.get_features_settings()

        self._chimera_use = features_settings['use_chimera']
        self._devial = features_settings['devial']

        self.not_parsed = True
        self.not_removed = True

    def __repr__(self) -> str:
        return f"Package: {self.name}"

    def getMetaObjects(self, filters: str) -> List[PackageObject]:
        """Верну метаданные после переименований по фильтру.

        Примеры фильтров:
            "Dag{r}" - даги с флагом r
            "Meta{s,n}" - все объекты с флагами s и n
            "Dag{s}|GpTable{b}" - даги с флагом s и gp-таблицы с флагом b
            "Meta{b,o}" - любая мета с флагом b и o
            "GpTable{b}|GpTable{n}" - любая gp-таблица с флагом b или n
            "Dag{s,!n}" - даги с флагом s, но без флага n
            "Dag{!i,!o}" - все даги без флагов i и o
            "Meta{}" - все метаданные

        :param filters: фильтр
        :return: список метаданных
        """
        return self._pack.getMetaObjects(filters)

    def getMetaObjectsBeforeAlter(self, filters: str) -> List[PackageObject]:
        """Верну метаданные до переименований по фильтру.

        Примеры фильтров:
            "Dag{r}" - даги с флагом r
            "Meta{s,n}" - все объекты с флагами s и n
            "Dag{s}|GpTable{b}" - даги с флагом s и gp-таблицы с флагом b
            "Meta{b,o}" - любая мета с флагом b и o
            "GpTable{b}|GpTable{n}" - любая gp-таблица с флагом b или n
            "Dag{s,!n}" - даги с флагом s, но без флага n
            "Dag{!i,!o}" - все даги без флагов i и o
            "Meta{}" - все метаданные

        :param filters: фильтр
        :return: список метаданных
        """
        return self._pack.getMetaObjectsBeforeAlter(filters)

    def init_type(self):
        pass

    def init_package_object(self):
        pass

    def remove(self):
        if self.not_removed:
            remove_cmd = 'rm -rf {0}'.format(self.path)
            remove_rc, remove_log = SystemCommand.execute(remove_cmd)
            self.not_removed = False

    def _init_scenario_by_parsing_svn_pack(self):
        scenario_json_data = PackageUtils.parseScenarioByPath(os.path.join(self.path, pack_path.scn))
        for result in scenario_json_data:
            task_name = result['task_name']
            task_position = result['task_position']
            objects_for_task = result['objects_for_task']
            additional_options = result['additional_options']
            self._pack.addTask(task_name, task_position, objects_for_task, additional_options)

    # ToDO need work
    def _init_scenario_by_chimera(self):
        scenario = ChimeraApi().get_package_scenario(self.name)
        for step in scenario:
            step_id = step['step_id']
            step_type = step['step_type']
            step_index = step['step_index']
            parameters = step['parameters']
            stage = step['stage']
            plugin = step['plugin']
            additional_options = {'is_idempotent': step['is_idempotent'], 'is_user_defined': step['is_user_defined'], 'is_valid': step['is_valid']}
            self._pack.addTask(step_id, step_type, step_index, parameters, stage, plugin, additional_options)

    def _initScenario(self):
        if self._chimera_use == 'no':
            self._init_scenario_by_parsing_svn_pack()
        else:
            self._init_scenario_by_chimera()

    def parsePackage(self):
        if self.not_parsed:
            if self._chimera_use == 'no':
                self._pack.parsePackage()
            else:
                api = ChimeraApi()
                self._pack.parsePackage(api)
            self._initScenario()
            self.not_parsed = False

    def get_scenario_steps(self):
        return self._pack._TaskList

    def get_scenario_objects(self, scenario_name):
        return self._pack.getObjectsByTaskName(scenario_name)

    def get_scenario_additionalOptions(self, scenario_name):
        return self._pack.getTaskAdditionalOptions(scenario_name)


class SASpackage(BasePackage):
    def __init__(self, name, path, no_connect=False):
        super(SASpackage, self).__init__(name, path)

        self._contour = enviroment.local['tag_small']
        self.__cashed_targets_gptables_with_dep_dags = None
        self.__cashed_targets_dlhtables_with_dep_dags = None
        self.__cashed_TargetsWithDepJobsMG = None
        self.__cashed_ChangeColumsOfTables = OrderedDict()
        self.__cashed_jobs_deploys = {}
        self.__cashed_table_phys_in_pack = set()
        self.__cashed_dlh_table_phys_in_pack = set()
        self.__cashed_prod_load_params = []
        self.__cashed_dd_relations = dict()
        self.__cashed_columns_with_types = dict()
        self.__cashed_columns_with_types_for_dlh = dict()

        self._increment_settings_table = dict()

        if not no_connect:
            self.GPConnection = GreenPlumSQL(enviroment.local['gp']['Host'],
                                             enviroment.local['gp']['Port'],
                                             enviroment.local['gp']['DbName'], user.GPLogin,
                                             user.GPPassword)

        g_conf = GlobalConfig()
        try:
            if not no_connect:
                self.gp_prod_conn = GreenPlumSQL(enviroment.local['gp_prod']['Host'], enviroment.local['gp_prod']['Port'],
                                                 enviroment.local['gp_prod']['DbName'],
                                                 g_conf.gp_prod_user,
                                                 g_conf.gp_prod_password)
        except Exception as e:
            self.gp_prod_conn = None

    @property
    def contour(self) -> str:
        return self._contour

    @property
    def increment_size_table(self) -> dict:
        return self._increment_settings_table

    def pre_pickle_dump(self):
        self.GPConnection = ''
        self.gp_prod_conn = ''

    def post_pickle_load(self):
        self.GPConnection = GreenPlumSQL(enviroment.local['gp']['Connections'][0]['Host'],
                                         enviroment.local['gp']['Connections'][0]['Port'],
                                         enviroment.local['gp']['Connections'][0]['DbName'], user.GPLogin,
                                         user.GPPassword)

        g_conf = GlobalConfig()
        try:
            self.gp_prod_conn = GreenPlumSQL(enviroment.local['gp_prod']['Host'], enviroment.local['gp_prod']['Port'],
                                             enviroment.local['gp_prod']['DbName'],
                                             g_conf.SystemUser,
                                             g_conf.SystemUserPassword)
        except Exception as e:
            self.gp_prod_conn = None

    def init_type(self):
        self.type = 'SAS'

    def _init_with_old_svn_api(self):
        self._pack = Package(self.name, self.path, enviroment.local['paths']['TempPath'])

    def _init_with_chimera(self):
        self._pack = PackageChimeraAdapter(self.name, self.path, enviroment.local['paths']['TempPath'])

    def _get_cut2_settings_from_Chimera(self):
        api = ChimeraApi()
        self.cut2_settings = [Cut2Settings(obj["name"], type="cut2") for obj in api.get_package_objects(self.name)['items'] if
                              obj["plugin"] == "cut2"]

    def init_package_object(self):
        """
        Инициализирую тип пакета, объект для непосредственной работы с пакетом и отбираю скрипты в пакете
        """
        self.init_type()
        if self._chimera_use == 'no':
            self._init_with_old_svn_api()
        else:
            self._init_with_chimera()
        self.sas_scripts = [os.path.join(root_directory, file) for root_directory, child_directories, files in
                            os.walk(self.path) for file in files if
                            file.endswith('.sas') and os.path.relpath(root_directory, self.path).split(os.sep)[
                                        0] != 'backup']
        if self._chimera_use == 'no':
            self.chimera_sql_scripts = []
            self.chimera_dlh_sql_scripts = []
            self.cut2_settings = [Cut2Settings(os.path.join(root_directory, file)) for root_directory, child_directories, files in
                                  os.walk(os.path.join(self.path, 'release', 'cut2')) for file in files if
                                  file.endswith('.json')]

        else:
            files = ChimeraCollector.get_task_files(self.name, filter='.sql')
            self.chimera_sql_scripts = [file for file in files if not file.path.startswith('tests/')
                                        and not file.path.startswith('dlh')]
            self.chimera_dlh_sql_scripts = [file for file in files if file.path.startswith('dlh')]
            self._get_cut2_settings_from_Chimera()

    def get_ddl_scripts_for_syntetic(self):
        files = ChimeraCollector.get_task_files(self.name, filter='.sql')
        scripts = [file for file in files if file.path.startswith('tests/') and 'ddl ' in file.name]

        return scripts

    @catch_problem
    def _gen_table_by_meta_name_from_factory(self, name: str, prefix: str = 'test') -> TableDetail:
        table = self.table_factory.generate_table_by_meta_name(name, prefix)

        return table

    def get_all_package_objects_names(self):
        names = set()
        for meta_obj in self._pack.getMetaObjects(filters='Meta{}'):
            if '<>' in meta_obj.Name:
                names.add(meta_obj.Name.replace('<>', 'prod'))
            else:
                names.add(meta_obj.Name)
        for scenario_task in self._pack.getTasksByName('obsolete_gp_table'):
            for object_in_task in scenario_task["objects"]:
                name = object_in_task
                if '<>_' in object_in_task:
                    names.add(name.replace('<>_', 'prod_'))
                    name = name.split('<>_')[-1]
                if 'nifi.' in name:
                    names.add(name)
                    names.add(name.replace('nifi.', 'nifi_raw.'))
                else:
                    names.add(name)
        return names

    def get_all_dd_sources_of_dag(self, dag_name):
        mg_helper = self._get_mg_utils_depence()
        sources_urns = []
        dag_urn = mg_helper.get_urn_of_object(dag_name, type='DAG')
        if dag_urn:
            for source in mg_helper.get_etl_sources_by_urn(dag_urn[0][0]):
                table_urn = mg_helper.get_urn_of_object(source[0])
                if table_urn:
                    sources_urns.append(table_urn[0][0])
        return sources_urns

    def _get_all_dd_relations(self):
        names = self.get_all_package_objects_names()
        mg_helper = self._get_mg_utils_depence()
        relations_raw = mg_helper.get_all_relations_objects_by_names(names)
        all_relations = dict()
        for raw in relations_raw:
            if not all_relations.get(raw['entity_name']):
                all_relations[raw['entity_name']] = {'type': raw['entity_type'],
                                                     'urn': raw['urn'],
                                                     'sources': dict(),
                                                     'destinations': dict()}
            if not all_relations[raw['entity_name']]['sources'].get(raw['source_rel_type']):
                all_relations[raw['entity_name']]['sources'][raw['source_rel_type']] = {'hash_list': set(),
                                                                                        'objects': []}

            if not all_relations[raw['entity_name']]['destinations'].get(raw['dest_rel_type']):
                all_relations[raw['entity_name']]['destinations'][raw['dest_rel_type']] = {'hash_list': set(),
                                                                                           'objects': []}

            if raw['source_name'] and raw['source_name'] not in all_relations[raw['entity_name']]['sources'][raw['source_rel_type']]['hash_list']:
                all_relations[raw['entity_name']]['sources'][raw['source_rel_type']]['hash_list'].add(
                    raw['source_name'])
                all_relations[raw['entity_name']]['sources'][raw['source_rel_type']]['objects'].append(
                    {'name': raw['source_name'],
                     'urn': raw['source_urn'],
                     'type': raw['source_type'],
                     'attribute': raw['source_attribute']})

            if raw['destination_name'] and raw['destination_name']not in all_relations[raw['entity_name']]['destinations'][raw['dest_rel_type']]['hash_list']:
                all_relations[raw['entity_name']]['destinations'][raw['dest_rel_type']]['hash_list'].add(
                                                                                      raw['destination_name'])
                all_relations[raw['entity_name']]['destinations'][raw['dest_rel_type']]['objects'].append(
                    {'name': raw['destination_name'],
                     'urn': raw['destination_urn'],
                     'type': raw['destination_type'],
                     'attribute': raw['dest_attribute']})

        return all_relations

    def get_all_package_object_relations_from_dd(self):
        if not self.__cashed_dd_relations:
            self.__cashed_dd_relations = self._get_all_dd_relations()
        return self.__cashed_dd_relations

    def get_modify_tables_from_scripts(self):
        modified_tables = set()
        pref = self.table_factory.prefix
        ex_list = [self._gen_table_by_meta_name_from_factory(t, pref) for t in
                   names.UTL_META_TABLE_NAMES]
        excluded_tables = [t.phys_name for t in ex_list if (t and t.phys_name)]
        WRK_SCHEMA = 'test_etl_wrk'

        try:
            if self.chimera_sql_scripts:
                for table in ChimeraSqlScriptParser.find_all_modified_tables_from_list_of_scripts(
                        self.chimera_sql_scripts):
                    mod_schema = table.split('.')[0]
                    if table not in excluded_tables and mod_schema != WRK_SCHEMA:
                        modified_tables.add(table)

        except Exception as e:
            pass

        return modified_tables

    def get_modify_dlh_tables_from_scripts(self):
        modified_tables = set()
        pref = self.table_factory.prefix
        ex_list = [self._gen_table_by_meta_name_from_factory(t, pref) for t in
                   names.UTL_META_TABLE_NAMES]
        excluded_tables = [t.phys_name for t in ex_list if (t and t.phys_name)]
        WRK_SCHEMA = 'test_etl_wrk'

        try:
            if self.chimera_sql_scripts:
                for table in ChimeraSqlScriptParser.find_all_modified_tables_from_list_of_scripts(
                        self.chimera_dlh_sql_scripts):
                    mod_schema = table.split('.')[0]
                    if table not in excluded_tables and mod_schema != WRK_SCHEMA:
                        modified_tables.add(table)

        except Exception as e:
            pass

        return modified_tables

    @catch_problem
    def get_table_phys_in_pack(self):
        if self.__cashed_table_phys_in_pack:
            return self.__cashed_table_phys_in_pack
        else:
            gp_tables = self.getMetaObjects('GpTable{}')
            gp_tables_phys = set()
            for table in gp_tables:
                if '_utl_md.' not in table.Name:
                    gp_tables_phys.add(table.Name.replace('<>', self.table_factory.prefix))

            modified_tables = self.get_modify_tables_from_scripts()

            all_tables = modified_tables | gp_tables_phys

            self.__cashed_table_phys_in_pack = all_tables

            return all_tables

    @catch_problem
    def get_dlh_table_phys_in_pack(self):
        if self.__cashed_dlh_table_phys_in_pack:
            return self.__cashed_dlh_table_phys_in_pack
        else:
            dlh_tables = self.getMetaObjects('DlhTable{}')
            dlh_tables_phys = set()
            for table in dlh_tables:
                if '_utl_md.' not in table.Name:
                    dlh_tables_phys.add(table.Name.replace('<>', self.table_factory.prefix))

            modified_tables = self.get_modify_dlh_tables_from_scripts()

            all_tables = modified_tables | dlh_tables_phys

            self.__cashed_dlh_table_phys_in_pack = all_tables

            return all_tables

    @property
    def obsolete_jobs_load_params(self):
        return self.__cashed_prod_load_params

    @catch_problem
    def build_increment_settings_table(self):
        query = """
                select
                e.schema_nm || '.' || e.entity_nm as full_table_name
                , c.reltuples::bigint as row_cnt
                , e.size_gb as table_size
                from (SELECT dwh_rep_entity.valid_from_dttm, dwh_rep_entity.valid_to_dttm,
                             dwh_rep_entity.database_nm, dwh_rep_entity.schema_nm,
                             dwh_rep_entity.entity_nm,
                (((((dwh_rep_entity.table_size_byte)::numeric / 1024.0) / 1024.0) / 1024.0))::numeric(12,5) AS size_gb,
                             dwh_rep_entity.server_nm, dwh_rep_entity.owner_nm 
                             FROM prod_dds.dwh_rep_entity) e
                join pg_tables t
                    on e.schema_nm = t.schemaname and e.entity_nm = t.tablename
                join pg_class c
                    on (e.schema_nm || '.' || e.entity_nm)::regclass::oid = c.oid
                where e.schema_nm || '.' || e.entity_nm in ({0})
                    and valid_to_dttm = '5999-01-01'
                    and e.size_gb is not Null
                and (c.reltuples::bigint > {1} or e.size_gb > {2})
                and e.server_nm <> 'BT'
                order by table_size desc;
                """

        consul_api = ConsulAPIClient()
        settings = consul_api.get_increment_settings()

        all_tables = self.get_table_phys_in_pack()

        pref = self.table_factory.prefix

        tables_params = "'" + "','".join([table.replace(pref, 'prod') for table in all_tables]) + "'"

        execute_query = query.format(tables_params, settings['rows_count'], settings['gb_size'])

        if self.gp_prod_conn:
            try:
                rows_tables_size = self.gp_prod_conn.executeAndReturnLists(execute_query)
            except Exception as e:
                rows_tables_size = {}
        else:
            rows_tables_size = {}
        for row in rows_tables_size:
            if not self._increment_settings_table.get(row[0]):
                self._increment_settings_table[row[0]] = {'count': 0, 'size': 0}

            if (int(row[1]) >= self._increment_settings_table[row[0]]['count'] and float(row[2]) >=
                    self._increment_settings_table[row[0]]['size']):
                self._increment_settings_table[row[0]]['count'] = int(row[1])
                self._increment_settings_table[row[0]]['size'] = float(row[2])

    @catch_problem
    def _get_tables_from_sas_scripts(self, target_operator=None, return_id_translate_only=False) -> List[str]:
        """
        Верну список таблиц, которые используются в SAS скриптах пакета
        отбираю тллько 'ALTER TABLE', 'CREATE TABLE', 'CREATE VIEW', 'REPLACE VIEW', 'DROP TABLE'
        исключаю таблицы в ворковых схемах, в UTL схемах
        если return_id_translate_only==True и меняются id_translat-ы, то верну только их
        :return: список, пример: [DWH_GP_VAULT_SCHEMA..S_APPLICATION_ILOG_CHNG, DWH_GP_ILOG_OOD_SCHEMA..DECISION_ARCH]
        """
        list_of_analyzed_table: List[str] = []

        if target_operator:
            find_operators = target_operator
        else: find_operators = ['ALTER TABLE', 'CREATE TABLE', 'CREATE VIEW', 'REPLACE VIEW', 'DROP TABLE']

        for script in self.sas_scripts:

            with open(script, errors='ignore') as script_code:
                for line in script_code:
                    up_line = line.upper()

                    for operator in find_operators:
                        if operator in up_line and '&' in up_line:
                            if '\t' in up_line:
                                analyzed_table_tmp = up_line.replace('\t', ' ').split('&')[1].split(' ')[0]
                                analyzed_table = analyzed_table_tmp.replace(';', '').strip()
                            else:
                                analyzed_table = up_line.split('&')[1].split(' ')[0].replace(';', '').strip()

                            schema = analyzed_table.split('..')[0]

                            translate_flg = True if (schema == names.UtlSchema and return_id_translate_only
                                                     and 'ID_TRANSLATE' in analyzed_table) else False

                            if (schema != names.WrkSchema and schema != names.UtlSchema and not return_id_translate_only) \
                                    or translate_flg:
                                if analyzed_table in list_of_analyzed_table:
                                    continue

                                list_of_analyzed_table.append(analyzed_table)

        return list_of_analyzed_table

    @catch_problem
    def get_sql_scripts_path(self, scripts_directory='release') -> List[str]:
        """
        Верну список путей всех sql-скриптов из указанной директории в пакете
        :param: scripts_directory - имя папки в пакете, по умолчанию - 'release'
        :return: список, пример: [before_import_ch.sql, create_tales_ch.sql]
        """
        scripts_path: List[str] = []

        search_dir = os.path.join(self.path, scripts_directory)
        # если scripts_directory не существует, ищем по всему пакету
        work_dir = search_dir if os.path.exists(search_dir) and os.path.isdir(search_dir) else self.path

        # рекурсивно обходим вложенные папки и достаем .sql скрипты
        for root, dirs, files in os.walk(work_dir):
            for file in files:
                if file.endswith(".sql"):
                    scripts_path.append(os.path.join(root, file))
        return scripts_path

    @catch_problem
    def find_tables_with_changing_diff_in_scripts(self, command='tcs_harmonize_diff_table') -> List[str]:
        """
        Верну список таблиц, для которых в SAS скриптах пакета меняется diff-таблица.
        (отбираю только скрипты, которые работают на всех контурах)
        :param: command - имя функции, которая меняет diff-table
        :return: список, пример: [DWH_GP_VAULT_SCHEMA..S_APPLICATION_ILOG_CHNG, DWH_GP_ILOG_OOD_SCHEMA..DECISION_ARCH]
        """
        tables: List[str] = []
        pattern = r'.*SELECT.*{0}.*\(.*&(.+?)\).*'.format(command.upper())

        # Учитываем только скрипты, запускаемые на всех контурах, т.к. измеение diff-ов д.б. на всех конрурах
        scripts = self._pack.getObjectsByTaskName('ExecuteSasScriptOnAllContours')
        package_scripts = self.sas_scripts

        # Получаем полные пути к таким скриптам
        all_cont_scr = []
        for name in scripts:
            for ps in package_scripts:
                if name in ps:
                    all_cont_scr.append(ps)

        for file in all_cont_scr:
            with open(file, errors='ignore') as script_code:
                for line in script_code:
                    up_line = line.upper()
                    tab = re_find(pattern, up_line)
                    if tab:
                        tables.append(str.strip(tab[0]))

        return tables

    @catch_problem
    def find_jobs_in_schedule(self) -> List[str]:
        """
        Верну список всех джобов из шага Schedule в сценарии.
        """
        jobs_in_schedule = self._pack.getObjectsByTaskName('Schedule')

        return jobs_in_schedule

    # TODO ужас переписать, найти способ определять переименнованные таблицы без привязки к backup_ddl
    @catch_problem
    def __get_rename_tables_test(self) -> List[str]:
        """
        метод реализует логику отбора на тестовом контуре
        :return: список, пример: [GP_VAULT.S_APPLICATION_ILOG_CHNG, GP_OOD.DECISION_ARCH]
        """
        table_results: List[str] = []

        try:
            with open(r'{0}/{1}'.format(self.path, pack_path.bckp_ddl)) as f:
                ddl_text = f.read()
            try:
                gp_table = [tbl.split(';')[0].split('.')[1] for tbl in ddl_text.split('-- DROP TABLE ')[1:]]
            except IndexError:
                gp_table: List[str] = []
        except FileNotFoundError:
            gp_table: List[str] = []

        meta_old_paths = [options['additional_options']['old_meta_name_and_path'] for options in self._pack._TaskList
                          if options['name'] == 'AlterMetadata' and 'Tables' in options['additional_options'][
                              'old_meta_name_and_path']]

        meta_names = [(path.split('/')[-1].replace(' ', '_').lower(),
                       SASNameTransfer.get_lib_name_by_meta_name(path.split('/')[2])) for path in meta_old_paths]

        for meta in meta_names:
            for table in gp_table:
                if table in meta[0] and table[-10:] == meta[0][-10:]:
                    table_results.append('{0}.{1}'.format(meta[1], table.upper()))

        return table_results

    @catch_problem
    def __get_rename_tables_vial(self) -> List[str]:
        """
        метод реализует логику отбора на виальном контуре
        :return: список, пример: [GP_VAULT.S_APPLICATION_ILOG_CHNG, GP_OOD.DECISION_ARCH]
        """
        table_results: List[str] = []
        prefix = get_prefix(self._contour, self.name)

        try:
            query_all_tables = """
            select 
            b.table_full_name
            from test_aru.at_phys_data a 
            join  test_aru.bfr_translation b on a.backup_key=b.table_backup_key
            where a.task_id = '{0}'
            ;
            """.format(prefix[2:])

            tables = [table[0].replace('prod_', '{0}_'.format(prefix)) for table in
                      self.GPConnection.executeAndReturnLists(query_all_tables)]

            gp_table: List[str] = []
            for table in tables:
                query_select = """
                select * from {0} limit 0;
                """.format(table)
                try:
                    self.GPConnection.executeAndReturnLists(query_select)
                except Exception:
                    gp_table.append(table.split('.')[1])
        except Exception:
            gp_table: List[str] = []

        meta_old_paths = [options['additional_options']['old_meta_name_and_path'] for options in self._pack._TaskList
                          if options['name'] == 'AlterMetadata' and 'Tables' in options['additional_options'][
                              'old_meta_name_and_path']]

        meta_names = [(path.split('/')[-1].replace(' ', '_').lower(),
                       SASNameTransfer.get_lib_name_by_meta_name(path.split('/')[2])) for path in meta_old_paths]

        for meta in meta_names:
            for table in gp_table:
                if table in meta[0] and table[-10:] == meta[0][-10:]:
                    table_results.append('{0}.{1}'.format(meta[1], table.upper()))

        return table_results

    def _getRename_tables(self) -> List[str]:
        """
        Верну список переименованных таблиц
        :return: список, пример: [GP_VAULT.S_APPLICATION_ILOG_CHNG, GP_OOD.DECISION_ARCH]
        """
        if self._contour == 'vial':
            return self.__get_rename_tables_vial()
        else:
            return self.__get_rename_tables_test()

    @catch_problem
    def _find_old_name_by_chimera(self, new_name: str) -> str:
        objects = ChimeraApi().get_package_objects(self.name)
        if objects.get('items'):
            for item in objects['items']:
                if item.get('new_name'):
                    if new_name == item.get('new_name'):
                        return item['name']
                if item['name'] == new_name:
                    return item['name']

    @catch_problem
    def _get_obj_name_before_renaming(self, current_name: str, obj_type: str) -> Optional[str]:
        """Верну строку с именем мета-объекта до переименования (такое, как на проде).

        Если объект не переименовывался, то верну текущее имя.
        Если объекта с таким именем нет в списке, то верну None.

        :param current_name: текущее имя на тестируемом контуре
        :param obj_type: тип объекта (Job, Dag, GpTable)
        :return: имя объекта до переименования.
            Пример: 'marti_load_usr_web_ast_pds_collect_calls'
        """
        if self._chimera_use == 'yes':
            obj_filter = obj_type + '{}'
        else:
            obj_filter = obj_type.capitalize() + '{!o}'

        obj_lst = self.getMetaObjectsBeforeAlter(obj_filter)
        old_name = None
        if obj_lst:
            for obj in obj_lst:
                if obj.Name and obj.NewMetaObject.Name == current_name:
                    old_name = obj.Name
                    break

        return old_name

    @catch_problem
    def get_job_name_before_renaming(self, current_name: str) -> Optional[str]:
        """Верну строку с именем джоба до переименования (такое, как на проде).

        :param current_name: текущее имя джоба на тестируемом контуре
        :return: имя джоба до переименования.
            Пример: 'EMART LOAD APPLICATIONS'
        """
        return self._get_obj_name_before_renaming(current_name, 'Job')

    @catch_problem
    def get_dag_name_before_renaming(self, current_name: str) -> Optional[str]:
        """Верну строку с именем дага до переименования (такое, как на проде).

        :param current_name: текущее имя дага на тестируемом контуре
        :return: имя дага до переименования.
            Пример: 'emart_load_financial_transaction'
        """
        return self._get_obj_name_before_renaming(current_name, 'Dag')

    @catch_problem
    def get_table_name_before_renaming(self, current_name: str) -> Optional[str]:
        """Верну строку с именем таблицы до переименования (такое, как на проде).

        :param current_name: текущее имя таблицы на тестируемом контуре
        :return: имя таблицы до переименования.
            Пример: 'EMART APPLICATIONS'
        """
        return self._get_obj_name_before_renaming(current_name, 'Table')

    @catch_problem
    def get_name_of_table_before_renaming(self, current_name: str, table_type: str, prefix: str = 'test') -> Optional[str]:
        """Верну строку с именем таблицы до переименования (такое, как на проде).

        Старое имя таблицы будет с тем же префиксом, что и в текущем имени.

        :param current_name: текущее имя таблицы на тестируемом контуре
        :param table_type: тип таблицы (Gptable/DlhTable)
        :param prefix: префикс в полном имени таблицы (по умолчанию 'test').
            Примеры: 'test', 'tdw52291'.
        :return: имя таблицы до переименования с тем же префиксом, что и в текущем имени.
            Пример: 'test_marti.usr_web_ast_pds_collect_calls'
        """
        prefix_flag = current_name.startswith(prefix)
        new_meta_name = current_name.replace(prefix, '<>', 1) if prefix_flag else current_name
        old_meta_name = self._get_obj_name_before_renaming(new_meta_name, table_type)

        if old_meta_name == new_meta_name:
            return current_name
        elif old_meta_name is None:
            return None

        old_phys_name = old_meta_name.replace('<>', prefix, 1) if prefix_flag else old_meta_name
        return old_phys_name

    def get_cashed_deploys(self):
        return self.__cashed_jobs_deploys

    @catch_problem
    def _cash_depend_jobs_deploys(self, dep_job_list):
        for job in dep_job_list:

            if not self.__cashed_jobs_deploys.get(job):
                parser = ParseDeployAndCheck(job, False, 'vial')

                try:
                    code = parser.get_deploy_code_from_etl_repository()
                    self.__cashed_jobs_deploys[job] = code
                except Exception as e:
                    pass

    @catch_problem
    def get_table_with_depend_jobs_mg(self) -> Dict[str, List[str]]:
        mg_helper = self._get_mg_utils_depence()
        result_dic: OrderedDict[str, List[str]] = OrderedDict()

        # вытягиваем джобы из пакета
        jobs = [job.Name for job in self.getMetaObjects('Job{!o}') if '[Obsolete]' not in job.Name and
                'OBSOLETE' not in job.Name.upper() and '[Manual]' not in job.Name and not
                job.Name.upper().startswith('MANUAL')]

        # получаем список измененных в задаче джобов; их не будем включать в зависимые, т.к. и так проверяем их
        if self._contour == 'vial':
            exclude_jobs = [p_job.Name for p_job in self.getMetaObjects('Job{i}') if '[Obsolete]' not in p_job.Name and
                'OBSOLETE' not in p_job.Name and '[Manual]' not in p_job.Name and not p_job.Name.startswith('MANUAL')]
        else:
            exclude_jobs = None

        for job in jobs:
            # на случай переименования объектов получаем имена до переименования
            prod_job_name = self.get_job_name_before_renaming(job)

            # через MG реплики для джоба получаем таргеты
            job_targets = {t[0] for t in mg_helper.get_target_phys_by_job_name(prod_job_name)}
            for t in job_targets:
                # получаем имя таблицы в формате либа.таблица, т.к. дальше зависимости используются в таком виде
                splits = t.split('.')
                meta_schema = ''
                if splits and len(splits) > 1:
                    underscores = splits[0].split('_')
                    if underscores and len(underscores) > 1 and underscores[1].upper() == 'INTEGRATION':
                        meta_schema = 'IMART'
                    elif (underscores and len(underscores) > 2 and underscores[1].upper() == 'DWH' and
                            underscores[2].upper() == 'REP'):
                        meta_schema = 'DWH REP'
                    elif underscores and len(underscores) > 1:
                        meta_schema = underscores[1].upper()

                    lib_n = str.format('{0}.{1}', SASNameTransfer.get_lib_name_by_meta_name(meta_schema),
                                       splits[1].upper())
                    # через MG получаем список зависимых на проде
                    urn = mg_helper.get_urn_of_object(t)
                    if urn:
                        list = mg_helper.get_depend_etl_objects_by_urn(urn[0][0])
                    else:
                        list = []
                    dep_jobs_list = []
                    for el in list:
                        try:
                            dep_job = el[0]
                            type = el[1]
                            if dep_job and type == 'JOB':
                                # джобы по задаче не учитываем
                                if ((exclude_jobs and dep_job in exclude_jobs)
                                    # также не учитываем ноуты в ZEP
                                    or (dep_job.upper().startswith('ZEPP'))
                                    # и потоки в информатике
                                    or (dep_job.upper().startswith('WF') and dep_job.split('_'))
                                     ):
                                    continue
                                else:
                                    dep_jobs_list.append(dep_job)
                        except KeyError:
                            dep_jobs_list = []
                    if dep_jobs_list:
                        self._cash_depend_jobs_deploys(dep_jobs_list)
                        result_dic[lib_n] = dep_jobs_list
        return result_dic

    def _get_mg_utils_depence(self):
        togle = FeaturesTogles()
        if togle.dd_replica_usage == 'test':
            return MgReplicaGpHelper(self.GPConnection, schema='test_aru')
        else:
            return MgReplicaGpHelper(self.gp_prod_conn)

    @catch_problem
    def _get_depend_tedi_dags_from_gp_source(self, gp_source):
        mg_helper = self._get_mg_utils_depence()

        table = gp_source.replace('test_', 'prod_', 1) if gp_source.startswith('test_') else gp_source
        urn_list = mg_helper.get_urn_of_object(table)
        all_depend_etl_objects_by_urn = []
        if urn_list:
            for urn in urn_list:
                table_urn = urn[0]
                if UrnPrefixes.UrnGpPrefix.value in table_urn:
                    all_depend_etl_objects_by_urn = mg_helper.get_depend_etl_objects_by_urn(table_urn)
                    break
        dep_dags_list = [etl_object[0] for etl_object in all_depend_etl_objects_by_urn if etl_object[1] == 'DAG']
        return dep_dags_list

    @catch_problem
    def _get_depend_tedi_dag_from_dlh_source(self, dlh_source):
        mg_helper = self._get_mg_utils_depence()

        table = dlh_source.replace('test_', 'prod_', 1) if dlh_source.startswith('test_') else dlh_source
        urn_list = mg_helper.get_urn_of_object(table)
        all_depend_etl_objects_by_urn = []
        if urn_list:
            for urn in urn_list:
                table_urn = urn[0]
                if UrnPrefixes.UrnDlhPrefix.value in table_urn:
                    all_depend_etl_objects_by_urn = mg_helper.get_depend_etl_objects_by_urn(table_urn)
                    break
        dep_dags_list = [etl_object[0] for etl_object in all_depend_etl_objects_by_urn if etl_object[1] == 'DAG']
        return dep_dags_list

    @catch_problem
    def get_dags_depend_on_table(self, table_name: str, table_type: str = 'gp') -> List[str]:
        """Верну список имён дагов на проде, у которых одним из источников является указанная таблица.

        :param table_name: имя таблицы
        :param table_type: тип таблицы, gp или dlh
        :return: список имён дагов
        """
        if table_type == 'gp':
            return self._get_depend_tedi_dags_from_gp_source(table_name)
        elif table_type == 'dlh':
            return self._get_depend_tedi_dag_from_dlh_source(table_name)

    @catch_problem
    def _get_tedi_dag(self, dag_name: str) -> TediDag:
        """Создам и верну объект класса TediDag, соответствующий переданному имени дага.

        :param dag_name: имя дага
        :return: экземпляр класса TediDag
        """
        if dag_name.startswith('manual_'):
            tedi_dag = TediDag(dag_name, branch=self.name, use_tedi_api=False, lazy=False)
            if not tedi_dag.meta:
                tedi_dag = TediDag(dag_name, branch='test', use_tedi_api=False, lazy=False, strong_branch=True)
        else:
            tedi_dag = TediDag(dag_name, branch=self.name)

        return tedi_dag

    @catch_problem
    def _get_tedi_dag_targets(self, tedi_dag: TediDag) -> Optional[List[str]]:
        """Верну список таблиц приемников переданного объекта TediDag.

        :param tedi_dag: экземпляр класса TediDag
        :return: список таблиц приемников дага
        """
        target_tables = []
        try:
            target_tables = tedi_dag.get_target_tables()
        except DagNotInRepository as e:
            print(e)
            raise e
        except AttributeError as e:
            pass

        return target_tables

    @catch_problem
    def _get_dag_targets(self, dag_name: str) -> Optional[List[str]]:
        """Верну список таблиц приемников по имени дага.

        :param dag_name: имя дага
        :return: список таблиц приемников дага
        """
        tedi_dag = self._get_tedi_dag(dag_name)
        target_tables = self._get_tedi_dag_targets(tedi_dag)

        return  target_tables

    @catch_problem
    def get_dag_targets(self, dag_name: str) -> Optional[List[str]]:
        """Верну список таблиц приемников по имени дага.

        :param dag_name: имя дага
        :return: список таблиц приемников дага
        """
        return  self._get_dag_targets(dag_name)

    @catch_problem
    def get_dags_with_target_tables(self, filters: str = 'Dag{}') -> Dict[str, List[str]]:
        """Верну словарь, в котором ключами являются имена дагов из пакета, отобранные по фильтру.
        А значениями по ключу являются списки таблиц приемников этих дагов.

        :param filters: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        :return: словарь с именами дагов и списком их таблиц приемников
        """
        package_dags = [dag.Name for dag in self.getMetaObjects(filters)]
        dags_with_target_tables = dict()

        for dag in package_dags:
            target_tables = self.get_dag_targets(dag)
            # На уровне дагов нет деления на DLH/GP и тд, поэтому считаем,
            # что если нет GP-таргета, то даг не возвращаем
            if target_tables:
                dags_with_target_tables[dag] = target_tables

        return dags_with_target_tables

    @catch_problem
    def get_dags_with_dlh_target_tables(self, filters: str = 'Dag{}') -> Dict[str, List[str]]:
        """Верну словарь, в котором ключами являются имена дагов из пакета, отобранные по фильтру.
        А значениями по ключу являются списки таблиц приемников этих дагов.

        :param filters: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        :return: словарь с именами дагов и списком их таблиц приемников
        """
        package_dags = [dag.Name for dag in self.getMetaObjects(filters)]
        dags_with_target_tables = dict()

        for dag in package_dags:
            target_tables = self.get_dag_dlh_targets(dag)
            # На уровне дагов нет деления на DLH/GP и тд, поэтому считаем,
            # что если нет DLH-таргета, то даг не возвращаем
            if target_tables:
                dags_with_target_tables[dag] = target_tables
        return dags_with_target_tables

    @catch_problem
    def get_targets_gptables_with_dep_dags(self, filters: str = 'Dag{}') -> Dict[str, List[str]]:
        """Верну словарь, в котором ключами являются имена gp-таблиц. Эти gp-таблицы, в свою очередь,
        являются приемниками для дагов из пакета, отобранных по переданному фильтру.
        А значениями по ключу являются списки дагов с прода, которые зависят от таблицы ключа.

        :param filters: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        :return: словарь с именами gp-таблиц, являющихся приемниками дагов из пакета, и списком дагов с прода,
            зависящих от этих таблиц
        """
        dags_with_target_tables = self.get_dags_with_target_tables(filters)
        targets_with_depend_dags = dict()

        for dag in dags_with_target_tables.keys():
            target_tables = dags_with_target_tables[dag]
            for table in target_tables:
                targets_with_depend_dags[table] = self.get_dags_depend_on_table(table)

        return targets_with_depend_dags

    def get_dag_dlh_targets(self, dag: str):
        """Получаем DLH таргеты дага"""
        tedi_dag = self._get_tedi_dag(dag)
        target_tables = tedi_dag.get_dlh_targets()

        return target_tables

    @catch_problem
    def get_targets_dlhtables_with_dep_dags(self, filters: str = 'Dag{}') -> Dict[str, List[str]]:
        """Верну словарь, в котором ключами являются имена dlh-таблиц. Эти dlh-таблицы, в свою очередь,
        являются приемниками для дагов из пакета, отобранных по переданному фильтру.
        А значениями по ключу являются списки дагов с прода, которые зависят от таблицы ключа.

        :param filters: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        :return: словарь с именами gp-таблиц, являющихся приемниками дагов из пакета, и списком дагов с прода,
            зависящих от этих таблиц
        """
        if not self.__cashed_targets_dlhtables_with_dep_dags:
            package_dags = [dag.Name for dag in self.getMetaObjects(filters)]
            dags_with_dlh_target_tables = dict()
            targets_with_depend_dags = dict()

            for dag in package_dags:
                target_tables = self.get_dag_dlh_targets(dag)
                dags_with_dlh_target_tables[dag] = target_tables

            for dag in dags_with_dlh_target_tables.keys():
                target_tables = dags_with_dlh_target_tables[dag]
                for table in target_tables:
                    targets_with_depend_dags[table] = self.get_dags_depend_on_table(table, table_type='dlh')

            self.__cashed_targets_dlhtables_with_dep_dags = targets_with_depend_dags
        return self.__cashed_targets_dlhtables_with_dep_dags

    @catch_problem
    def get_targets_gptables_with_old_names(self, filters: str = 'Dag{}') -> Dict[str, List[str]]:
        """Верну словарь, в котором ключами являются имена gp-таблиц. Эти gp-таблицы, в свою очередь,
        являются приемниками для дагов из пакета, отобранных по переданному фильтру.
        А значениями по ключу являются имена этих таблиц до переименования в пакете.
        Если таблица не переименовывалась, то значением будет текущее имя.

        :param filters: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        :return: словарь с именами gp-таблиц, являющихся приемниками дагов из пакета, и именами этих таблиц
            до переименования в пакете
        """
        dags_with_target_tables = self.get_dags_with_target_tables(filters)
        targets_with_old_names = dict()

        for dag in dags_with_target_tables.keys():
            target_tables = dags_with_target_tables[dag]
            for table in target_tables:
                table_old_name = self.get_name_of_table_before_renaming(table, 'GpTable')
                if table_old_name is not None:
                    targets_with_old_names[table] = table_old_name

        return targets_with_old_names

    @catch_problem
    def get_ext_attr_from_spk_file(self, job_name: str) -> Dict[str, str]:
        """
        Верну Extended Attributes джоба из его spk
        :param job: имя джоба
        :return словарь, где ключ - название параметра
        """
        spk_name = 'release/spk/job-{0}.spk'.format(job_name.replace(' ', '-').lower())

        root = self._base_spk_extract_and_parse(spk_name, 'TransportMetadata.xml')
        attr_lines = {}
        for src_code in root.iter('Extension'):
            try:
                attr_lines[src_code.attrib['Name']] = src_code.attrib['Value']
            except KeyError as e:
                pass

        return attr_lines

    @catch_problem
    def get_depend_jobs_by_table(self, table: str, prefix: str) -> List[str]:
        client = MgClient()
        if prefix in table:
            table = table.replace(prefix, 'prod')
        else:
            table = table.replace('test_', 'prod_')

        depend_jobs = client.get_depend_jobs_by_table_phys_name(table)
        depend_jobs_name = [i.get('entityName') for i in depend_jobs]

        return depend_jobs_name

    @catch_problem
    def _get_dep_jobs_without_deployment(self, dep_dict: Dict) -> set:
        dep_jobs_set = set()
        if dep_dict and isinstance(dep_dict, Dict):
            all_dep_jobs = set([el for val in dep_dict.values() for el in val])
            for job in all_dep_jobs:
                # main=False т.к. джобы зависимые!
                tech_job = self.job_factory(job, '', main=False, countour=self._contour)
                try:
                    deploy_code = tech_job.get_deploy_code()
                except (NoDeploy, CantGetDeployFromRepository):
                    deploy_code = ''
                finally:
                    if not deploy_code:
                        dep_jobs_set.add(job)
        return dep_jobs_set


    # ToDO сейчас берем ddl из пакета, переписать, когда ddl будут в стеше
    def _getTableBckpDdl_test(self, table: str) -> str:
        """
        Верну текст DDL таблицы для теста.
        :param table: имя таблицы, например - test_emart.financial_transaction
        :return DDL text или ''
        """
        try:
            with open(os.path.join(self.path, pack_path.bckp_ddl), errors='ignore') as ddl_file:
                readed_bckp_tables = ddl_file.read().split('-- ')[1:]
                for table_ddl in readed_bckp_tables:
                    if table + ';' in table_ddl:
                        return table_ddl
                return ''
        except IOError:
            return ''

    def get_bckp_table_for_vial(self, table: str, prefix: str) -> str:
        return self._get_bckp_table_for_vial(table, prefix)

    def _get_bckp_table_for_vial(self, table: str, prefix: str) -> str:
        """
        Верну имя таблицы, из которой ресторилась таблица для виала.
        :param table: имя таблицы, например - vld13111_emart.financial_transaction
        :param prefix: префикс для замены, например - vld13111
        :return имя таблицы или ''
        """
        try:
            prod_schema, prod_table = set_prefix_in_schema('prod', table)
            query_find_buff_table = f"""
            select bfr_schema||'.'||bfr_table as table
            from
            test_aru.vw_at_phys_data
            where task_id = '{prefix[2:]}'
            and 
            schema_name = '{prod_schema}'
            and 
            table_name = '{prod_table}'
            limit 1;
            """

            buff_table = self.GPConnection.executeAndReturnLists(query_find_buff_table)[0][0]

            return buff_table
        except Exception as e:
            return ''

    def get_ddl_of_table(self, table_name):
        try:
            query_detect_ddl = """
                        select * from ddl('{0}')
                        ;
                        """.format(table_name)

            ddl_text = '\n'.join([row[0] for row in self.GPConnection.executeAndReturnLists(query_detect_ddl)])
            return ddl_text
        except Exception as e:
            return ''

    # ToDO тоже не самый элегантный способ, переписать, когда придем к версионированию ddl в репозиториях
    def _getTableBckpDdl_vial(self, table: str, prefix: str) -> str:
        """
        Верну текст DDL таблицы для виала.
        :param table: имя таблицы, например - vld13111_emart.financial_transaction
        :param prefix: префикс для замены, например - vld13111
        :return DDL text или ''
        """
        try:
            schema, table_name = table.replace(prefix, 'prod').split('.')

            query_find_buff = f"select bfr_schema||'.'||bfr_table from test_aru.vw_at_phys_data " \
                              f"where task_id = '{prefix[2:]}' " \
                              f"and schema_name = '{schema}' " \
                              f"and table_name = '{table_name}' " \
                              f"limit 1;"

            buff_table = self.GPConnection.executeAndReturnLists(query_find_buff)[0][0]

            ddl_text = self.get_ddl_of_table(buff_table).replace(buff_table, table)

            return ddl_text
        except Exception as e:
            return ''

    def _getTableBckpDdl(self, table: str, prefix: str) -> str:
        """
        Верну текст DDL таблицы.
        :param table: имя таблицы, например - vld13111_emart.financial_transaction или test_emart.financial_transaction
        :param prefix: префикс для замены, например - vld13111 или test
        :return DDL text или ''
        """
        if self._contour == 'vial':
            return self._getTableBckpDdl_vial(table, prefix)
        else:
            return self._getTableBckpDdl_test(table)

    def getRenameTables(self) -> List[str]:
        return self._getRename_tables()

    def get_targets_gptables_with_dags(self):
        if not self.__cashed_targets_gptables_with_dep_dags:
            self.__cashed_targets_gptables_with_dep_dags = self.get_targets_gptables_with_dep_dags()
        return self.__cashed_targets_gptables_with_dep_dags

    def get_targets_dlh_tables_with_dags(self):
        return self.get_targets_dlhtables_with_dep_dags()

    def getTargetsWithDepJobs(self) -> Dict[str, List[str]]:
        if not self.__cashed_TargetsWithDepJobsMG:
            self.__cashed_TargetsWithDepJobsMG = self.get_table_with_depend_jobs_mg()
        return self.__cashed_TargetsWithDepJobsMG

    @catch_problem
    def _extract_spk(self, spk_file: str, xml_to_extract: str):
        rand_str = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        z = zipfile.ZipFile(os.path.join(self.path, spk_file), 'r')
        xml_path = z.extract(xml_to_extract, os.path.join(self.path, rand_str))

        unix_permission_cmd = "chmod -R 777 {0}".format(os.path.join(self.path, rand_str))
        rc_permission, permission_log = SystemCommand.execute(unix_permission_cmd)

        return xml_path

    @catch_problem
    def _base_spk_extract_and_parse(self, spk_file: str, xml_to_extract: str):
        """
        Разархивирую spk файл внутри пакета и распарсю необходимый xml внутри архива
        :param spk_file: имя spk файла вместе с путем внутри пакета
        :param xml_to_extract: xml файл, который надо распарсить
        :return список колонок
        """
        xml_path = self._extract_spk(spk_file, xml_to_extract)

        tree = etree.parse(xml_path)
        root = tree.getroot()

        return root

    @catch_problem
    def get_columns_from_spk_file(self, table_name: str, from_buffer=False) -> Set[str]:
        """
        Верну список колонок для таблицы из её spk
        :param table_name: имя таблицы, например - EMART FINANCIAL TRANSACTION
        :param from_buffer: признак того, что мета берется из буфера (по умолчанию из release)
        :return список колонок
        """
        dir = 'buffer' if from_buffer else 'release/spk'
        spk_name = '{0}/table-{1}.spk'.format(dir, table_name.replace(' ', '-').lower())

        root = self._base_spk_extract_and_parse(spk_name, 'TransportMetadata.xml')
        meta_columns = set()
        for column in root.iter('Column'):
            try:
                meta_columns.add(column.attrib['SASColumnName'].lower())
            except Exception as e:
                pass

        return meta_columns

    @catch_problem
    def get_job_code_from_spk_file(self, job_name: str) -> Set[str]:
        """
        Верну список строк с кодом для джоба из его spk
        :param job: имя джоба
        :return список строк с кодом
        """
        spk_name = 'release/spk/job-{0}.spk'.format(job_name.replace(' ', '-').lower())

        root = self._base_spk_extract_and_parse(spk_name, 'TransportMetadata.xml')
        code_lines = set()
        for src_code in root.iter('TextStore'):
            try:
                # Берем только те тэги <TextStore>, в имени которые есть слово code, т.к. именно там хранится (пре / пост) код джоба / трансформа, код UW-тр.
                # Например: Name - "SourceCode", "UserWrittenSourceCode".
                if 'code' in src_code.attrib['Name'].lower():
                    code_lines.add(src_code.attrib['StoredText'].lower())
            except Exception as e:
                pass

        return code_lines

    @catch_problem
    def get_sticky_note_with_cut2(self, job_name: str):
        """
        Верну '<text>cut2</text>' если стикер с кат2 найден
        """
        sticker = None
        spk_name = 'release/spk/job-{0}.spk'.format(job_name.replace(' ', '-').lower())
        xml_path = self._extract_spk(spk_name, 'TransportMetadata.xml')
        for event, elem in etree.iterparse(xml_path):
            attr_with_sticker = elem.get('DefaultValue')
            if isinstance(attr_with_sticker, str):
                attr_with_sticker = attr_with_sticker.lower()
                sticker = re_find(r'<text>.*cut.*2.*</text>', attr_with_sticker)
                if sticker:
                    return sticker[0]
        return sticker

    def _get_phys_name_for_table_with_sas_lib(self, table: str):
        prefix = get_prefix(self._contour, self.name)

        phys_name = '{0}.{1}'.format(
            SASNameTransfer.get_shema_name_by_lib_name(table.split('.')[0], param=prefix),
            table.split('.')[1]).lower()

        return phys_name

    def _get_phys_name_for_gp_table_from_config(self, table: str, forced_contr: Union[str, None] = None):
        """

        Parameters
        ----------
        table: таблицв
        forced_contr: контур, используемый вместо переданного Химерой

        Returns
        -------
        phys_name: имя физической таблицы
        """
        prefix = get_prefix(self._contour, self.name, forced_contr)

        phys_name = table.replace('<>', prefix)
        if phys_name.startswith('test_'):
            phys_name = phys_name.replace('test_', prefix + '_')

        return phys_name

    def _get_all_columns_with_types(self, contour_prefix=None):
        try:
            tables_with_columns = dict()
            prefix = self.table_factory.prefix
            if prefix.startswith('t'):
                task_stage = 'test'
            elif prefix.startswith('pl'):
                task_stage = 'prodlike'
            else:
                task_stage = 'dev'
            gp_vial_objects = ChimeraApi().get_vial_objects(self.name, f'gp.{task_stage}.{self.name}')
            tables = [table["name"].replace('<>_', f'{prefix}_').split('.') for table in gp_vial_objects["items"]]
            bckp_tables = [table["backup_id"].split('.') for table in gp_vial_objects["items"] if table["backup_id"]]
            all_tables = tables + bckp_tables
            query = """select a.table_schema || '.' || a.table_name as tab, a.column_name, a.data_type, a.character_maximum_length
                        from information_schema.columns as a
                        where 
                        """
            params = ' or '.join([f"(a.table_schema = '{a}' and a.table_name = '{b}')" for a, b in all_tables])

            res = self.GPConnection.executeAndReturnLists(query + params + ';')

            for row in res:
                if not tables_with_columns.get(row[0]):
                    tables_with_columns[row[0]] = []
                tables_with_columns[row[0]].append((row[1], row[2], row[3]))
            return tables_with_columns

        except Exception as e:
            a = dict()
            return a

    def _get_all_columns_with_types_for_dlh(self, contour_prefix=None):
        try:
            tables_with_columns = dict()
            prefix = self.table_factory.dlh_prefix
            if prefix.startswith('vp_'):
                task_stage = 'dev'
            else:
                task_stage = 'test'
            dlh_vial_objects = ChimeraApi().get_vial_objects(self.name, f'dlh.{task_stage}.{self.name}')
            tables = [table["name"].replace('<>_', f'{prefix}_').split('.') for table in dlh_vial_objects["items"]]
            bckp_tables = [table["backup_id"].split('.') for table in dlh_vial_objects["items"] if table["backup_id"]]
            all_tables = tables + bckp_tables
            spark_sql = SparkSQL()
            for table in all_tables:
                columns_full_info = spark_sql.get_columns_full_info(f'{table[0]}.{table[1]}')
                tables_with_columns.update(columns_full_info)

            return tables_with_columns

        except Exception as e:
            a = dict()
            return a

    def get_all_columns_with_types(self):
        if not self.__cashed_columns_with_types:
            self.__cashed_columns_with_types = self._get_all_columns_with_types()
        return self.__cashed_columns_with_types

    def get_all_columns_with_types_for_dlh(self):
        if not self.__cashed_columns_with_types_for_dlh:
            self.__cashed_columns_with_types_for_dlh = self._get_all_columns_with_types_for_dlh()
        return self.__cashed_columns_with_types_for_dlh

    @catch_problem
    def _get_changed_columns_for_dlh_table(self, table: str):
        """
        Parameters
        ----------
        table: имя таблицы, ожидаем форматы: <>_tab.table_name, test_tab.table_name, vp_dw12345_tab.table_name

        Returns
        _______
        Массивы полей: добавленными, удаленными/измененными и ДО доработки
        """
        if self.__cashed_ChangeColumsOfTables.get(table):
            return self.__cashed_ChangeColumsOfTables.get(table)
        else:
            bckp_columns = {}
            now_columns = {}
            compare_ddl = CompareDdl()
            try:
                prefix = self.table_factory.dlh_prefix
                table_detail = self.table_factory.generate_table_by_test_name(table, prefix, table_type='dlh')
                table = self.table_factory()
                phys_table = table.phys_name
                now_columns = table_detail.get_columns_from_phys()
                # пока хардкодим, но среды могут измениться
                task_stage = 'prodlike'
                bckup_name = None
                dlh_vial_objects = ChimeraApi().get_vial_objects(self.name, f'dlh.{task_stage}.{self.name}')
                for vial_obj in dlh_vial_objects['items']:
                    # сравниваем имя таблицы с объектом из конфига
                    if vial_obj['name'] == table:
                        bckup_name = vial_obj['backup_id']
                    else:
                        # либо ищем имя физики в параметрах vial объекта
                        if vial_obj.get('extra_params'):
                            for param in vial_obj.get('extra_params'):
                                if phys_table == param['value']:
                                    bckup_name = vial_obj['backup_id']
                if bckup_name:
                    pref = bckup_name[:bckup_name[bckup_name.index('_'):]]
                    bckp_table = self.table_factory.generate_table_by_test_name(bckup_name, pref, table_type='dlh')
                    bckp_columns = bckp_table.get_columns_from_phys()
            except Exception as e:
                raise DlhTable.CantDetectColumnsOfPhysTable(f'Cant detect columns for dlh table - {table}: {e}')
            finally:
                all_columns = compare_ddl.find_add_drop_rename_columns(now_columns, bckp_columns)
                self.__cashed_ChangeColumsOfTables[table] = all_columns
                return self.__cashed_ChangeColumsOfTables[table]

    @catch_problem
    def getChangeColumsOfTable(self, table: str, table_type='GpTable', contour_prefix: Union[str, None] = None,
                               old_columns: Union[str, None] = None) -> Tuple[List[str], List[str], List[str]]:
        """
        Parameters
        ----------
        table: имя таблицы
        table_type: тип таблицы
        contour_prefix: префикс пробирки в зависимости от стадии задачи
        old_columns: набор полей ДО доработки (передается для Bind дагов)

        Returns
        _______
        Массивы полей: добавленными, удаленными/измененными и ДО доработки
        """
        if table_type == 'GpTable':
            if self.__cashed_ChangeColumsOfTables.get(table):
                return self.__cashed_ChangeColumsOfTables.get(table)

            else:
                bckp_columns = {}
                now_columns = {}
                compare_ddl = CompareDdl()
                try:
                    prefix = self.table_factory.prefix
                    phys_name = self._get_phys_name_for_gp_table_from_config(table, prefix)
                    now_columns = compare_ddl.get_set_of_columns_for_given_table(phys_name)
                    if self._contour != 'test':
                        # Если аргумент не передан, значит это не Bind даг и набор полей получаем из бэкапа
                        if not old_columns:
                            if prefix.startswith('t'):
                                task_stage = 'test'
                            elif prefix.startswith('pl'):
                                task_stage = 'prodlike'
                            else:
                                task_stage = 'dev'

                            gp_vial_objects = ChimeraApi().get_vial_objects(self.name, f'gp.{task_stage}.{self.name}')
                            bckp_table = None
                            for gp_vial_object in gp_vial_objects['items']:
                                if table == gp_vial_object['name'] or \
                                        phys_name.replace(prefix, '<>') == gp_vial_object['name']:
                                    bckp_table = gp_vial_object['backup_id']
                                    break
                            bckp_columns = compare_ddl.get_set_of_columns_for_given_table(bckp_table)
                        else:
                            bckp_columns = old_columns
                    else:
                        # Для UNIT-теста
                        bckp_columns = compare_ddl.getColumsFromDDLtext(phys_name, self._getTableBckpDdl(phys_name, prefix))
                except Exception as e:
                    raise TableDetail.CantDetectPhysTable(f'Cant detect phys for table (maybe this is not a GP-table). : {e}')
                finally:
                    all_columns = compare_ddl.find_add_drop_rename_columns(now_columns, bckp_columns)
                    self.__cashed_ChangeColumsOfTables[table] = all_columns
                    return self.__cashed_ChangeColumsOfTables[table]
        # ToDo поддержка типов таблиц, отличных от GP
        elif table_type == 'ClickHouse':
            return [], [], []
        elif table_type == 'DlhTable':
            return self._get_changed_columns_for_dlh_table(table)
        else:
            return [], [], []

    # прокидываем "фабрики" для создания объектов через единый package_analyzer
    @property
    def job_factory(self) -> Callable[..., JobDetail]:
        return JobDetail

    @property
    def tedi_dag_factory(self) -> Callable[..., TediDag]:
        if self.contour in ('chimera_dev', 'chimera_prodlike'):
            from Config.special_configs import BigTediConfig
            from Framework.utils.chimera_util import get_tedi_custom_url, ChimeraApi

            api = ChimeraApi()
            devial_tedi_url = get_tedi_custom_url(api, self.name)
            big_tedi_custom_contour = 'DevelopmentContour'
            if self.contour == 'chimera_prodlike':
                big_tedi_custom_contour = 'ProdlikeContour'
            tedi_api_conf = BigTediConfig(big_tedi_custom_contour, custom_url=devial_tedi_url)
            tedi_api_conf.is_meta_vial = True
            TediDag.contour_default = big_tedi_custom_contour
            TediDag.custom_tedi_api_conf = tedi_api_conf

        return TediDag

    @property
    def table_factory(self) -> Callable[..., TableFactory]:
        if self._chimera_use == 'no':
            TableFactory.prefix = get_prefix(self.contour, self.name)
            TableFactory.dlh_prefix = get_prefix(self.contour, self.name, pref_type='dlh')
        else:
            if self._devial == 'yes':
                TableFactory.prefix = get_prefix(self.contour, self.name)
                TableFactory.dlh_prefix = get_prefix(self.contour, self.name, pref_type='dlh')
            else:
                TableFactory.prefix = get_prefix('chimera_test', self.name)
                TableFactory.dlh_prefix = get_prefix('chimera_test', self.name, pref_type='dlh')
        return TableFactory

    def cut2_util_factory(self) -> Cut2Util:
        if self._contour == 'vial' or self.contour == 'chimera_test':
            main_cut2_api = Cut2Util()
        if self._devial == 'yes':
            main_cut2_api = Cut2Util(contour='DevelopmentContour')
            if self._contour == 'chimera_prodlike':
                main_cut2_api = Cut2Util(contour='ProdlikeContour')
        return main_cut2_api


class BIpackage(BasePackage):
    def init_type(self):
        self.type = 'BI'


class Nifipackage(BasePackage):
    def init_type(self):
        self.type = 'NiFi'


class Infomaticapackage(BasePackage):
    def init_type(self):
        self.type = 'Infa'

    def init_package_object(self):
        self.init_type()
        self._pack = Package(self.name, self.path, enviroment.local['paths']['TempPath'])
