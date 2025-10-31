import copy
from collections import OrderedDict
from abc import ABC, abstractmethod
from typing import Optional, List, Union, Any, Tuple, Callable

from Framework.utils.Gitlab_etl_worker import GitlabMetaRepoFactory


class Params:
    def __init__(self, params=None, ids=None, params_names=None):
        if params_names or params or ids:
            self.value = OrderedDict()
            self.names = params_names
            for param, id in zip(params, ids):
                self.value[id] = [param] if isinstance(param, str) else param
        else:
            self.value = OrderedDict()
            self.names = ''
        self._iter_count = 0
        self._iter_id = []

    def filter_params(self, id, decision=False):
        if decision:
            del self.value[id]

    def __iter__(self):
        self._iter_count = 0
        self._iter_id = [key for key in self.value]
        return self

    def __next__(self):
        if self._iter_count < len(self.value):
            self._iter_count += 1
            return self._iter_id[self._iter_count - 1], self.value[self._iter_id[self._iter_count - 1]]
        else:
            raise StopIteration


local_cash = {}


class BaseParametrsBuild(ABC):

    class NoParametrizationForParam(Exception):
        pass

    def __init__(self, package_analyzer, area_tag, filters_objects):
        self.package_analyzer = package_analyzer
        self.base_param_map = {'table': '_table_from_pack',
                               'target': '_targets',
                               'job_depend,target': '_param_depend_job_and_target',
                               'tedi_dag': '_dag_from_package',
                               'gp_table': '_get_gp_tables_from_package',
                               'dlh_table': '_get_dlh_tables_from_package',
                               'tedi_gp_target': '_tedi_dags_targets',
                               'tedi_dag,tedi_gp_target': '_param_tedi_dag_and_target',
                               'tedi_dag,tedi_dlh_target': '_param_tedi_dag_and_dlh_target',
                               'depend_tedi_dag,tedi_gp_target': '_param_depend_tdi_dag_and_target',
                               'depend_tedi_dag,tedi_dlh_target': '_param_depend_tedi_dag_and_dlh_target',
                               'tedi_gp_target,target_old_name,rename_flag,depend_tedi_dag': '_param_target_and_old_name_and_depend_dag',
                               'tedi_dlh_target,target_old_name,rename_flag,depend_tedi_dag': '_param_dlh_target_and_old_name_and_depend_dag'
                               }

        self.params = Params()
        self.area_tag = area_tag

        self.filter_objects = filters_objects

    def get_filter_objects(self):
        if self.filter_objects:
            return self.filter_objects.split(",")
        else:
            return []

    def build_base_param(self, param: str = 'job_depend, target', filter: Optional[str] = None):
        param = param.replace(' ', '')
        if local_cash.get((param, filter)):
            params = copy.deepcopy(local_cash.get((param, filter)))
            self.params = params
        else:
            try:
                func = self.__getattribute__(self.base_param_map[param])
                func(filter)
                local_cash[(param, filter)] = copy.deepcopy(self.params)
            except KeyError:
                raise BaseParametrsBuild.NoParametrizationForParam("Can't find base param - {0}".format(param))
            except AttributeError:
                raise BaseParametrsBuild.NoParametrizationForParam("Can't find param func for param - {0}".format(param))

    def __collect_local_params(self, params: List[Union[Tuple, List[Any]]], ids: List[str], name: str):
        self.params = Params(params, ids, name)

    def _dag_from_package(self, filter: Optional[str] = None):
        fltr = filter if filter else 'Dag{!o}'
        dags = [dag.Name for dag in self.package_analyzer.getMetaObjects(fltr)]
        self.__collect_local_params(dags, dags, 'tedi_dag')

    def _table_from_pack(self, filter: Optional[str] = None):
        """Filter objects from package, filters like 'Table{!o}', more detail in package module."""

        fltr = filter if filter else 'Table{!o}'
        tables = [table.Name for table in self.package_analyzer.getMetaObjects(fltr)]
        self.__collect_local_params(tables, tables, 'table')

    def _get_gp_tables_from_package(self, filter: Optional[str] = None):
        """Filter objects from package, filters like 'GpTable{!o}', more detail in package module."""

        fltr = filter if filter else 'GpTable{!o}'
        tables = [table.Name for table in self.package_analyzer.getMetaObjects(fltr)]
        self.__collect_local_params(tables, tables, 'gp_table')

    def _get_dlh_tables_from_package(self, filter: Optional[str] = None):
        """Filter objects from package, filters like 'DlhTable{!o}', more detail in package module."""

        fltr = filter if filter else 'DlhTable{!o}'
        tables = [table.Name for table in self.package_analyzer.getMetaObjects(fltr)]
        self.__collect_local_params(tables, tables, 'dlh_table')

    def _targets(self, filter: Optional[str] = None):

        tables = [table for table in self.package_analyzer.getTargetsWithDepJobs()]
        self.__collect_local_params(tables, tables, 'table')

    def _tedi_dags_targets(self, filter: Optional[str] = None):
        """Tables - targets from package Tedi DAGs"""

        tables = [table for table in self.package_analyzer.get_targets_gptables_with_dags()]
        self.__collect_local_params(tables, tables, 'tedi_gp_target')

    def _param_tedi_dag_and_target(self, filter: Optional[str] = None):

        fltr = filter if filter else 'Dag{!o}'
        dags = [dag.Name for dag in self.package_analyzer.getMetaObjects(fltr)]

        ids: List[str] = []
        params: List[Tuple[str, str]] = []
        for dag in dags:
            tech_dag = self.package_analyzer.tedi_dag_factory(dag, branch=self.package_analyzer.name)
            list_of_targets = []
            try:
                list_of_targets = tech_dag.get_target_tables()
            except Exception as e:
                pass

            for target in list_of_targets:
                params.append((tech_dag.name, target))
                ids.append(f'{tech_dag}, {target}')

        self.__collect_local_params(params, ids, 'tedi_dag, tedi_gp_target')

    def _param_tedi_dag_and_dlh_target(self, filter: Optional[str] = None):
        fltr = filter if filter else 'Dag{!o}'
        dags = [dag.Name for dag in self.package_analyzer.getMetaObjects(fltr)]

        ids: List[str] = []
        params: List[Tuple[str, str]] = []
        for dag in dags:
            tech_dag = self.package_analyzer.tedi_dag_factory(dag, branch=self.package_analyzer.name)
            list_of_targets = []
            try:
                list_of_targets = tech_dag.get_dlh_targets()
            except Exception as e:
                pass

            for target in list_of_targets:
                params.append((tech_dag.name, target))
                ids.append(f'{tech_dag}, {target}')

        self.__collect_local_params(params, ids, 'tedi_dag, tedi_dlh_target')

    def _param_depend_job_and_target(self, filter: Optional[str] = None):
        table_with_dep = self.package_analyzer.getTargetsWithDepJobs()
        params = []
        ids = []
        repo_api = GitlabMetaRepoFactory(repo='SAS')
        for table in table_with_dep:
            for depend_job in table_with_dep[table]:
                obsol_job = 'OBSOLETE ' + depend_job
                if obsol_job not in repo_api.repo_list_in_text:
                    params.append([depend_job, table])
                    ids.append(', '.join([depend_job, table]))
        self.__collect_local_params(params, ids, 'job_name, table')

    def _param_depend_tdi_dag_and_target(self, filter: Optional[str] = None):
        """
        Создам набор параметров (зависимый даг, источник от которого зависит даг/таргет дага из пакета).
        Не учитывает переименование таргета дага в пакете. Поэтому, в этом случае, не найдет зависимых дагов в DataDetective.
        """
        tables_with_depend_dags = self.package_analyzer.get_targets_gptables_with_dags()
        params = []
        ids = []

        for table in tables_with_depend_dags:
            for depend_dag in tables_with_depend_dags[table]:
                params.append([depend_dag, table])
                ids.append(', '.join([depend_dag, table]))
        self.__collect_local_params(params, ids, 'depend_tedi_dag, tedi_gp_target')

    def _param_depend_tedi_dag_and_dlh_target(self, filter: Optional[str] = None):
        """
        Создам набор параметров (зависимый даг, источник от которого зависит даг/таргет дага из пакета).
        Не учитывает переименование таргета дага в пакете. Поэтому, в этом случае, не найдет зависимых дагов в DataDetective.
        """
        tables_with_depend_dags = self.package_analyzer.get_targets_dlh_tables_with_dags()
        params = []
        ids = []

        for table in tables_with_depend_dags:
            for depend_dag in tables_with_depend_dags[table]:
                params.append([depend_dag, table])
                ids.append(', '.join([depend_dag, table]))
        self.__collect_local_params(params, ids, 'depend_tedi_dag, tedi_dlh_target')

    def _param_target_and_old_name_and_depend_dag(self, filter: str = 'Dag{}') -> None:
        """Создам набор параметров.

         Набор параметров следующий:
         (gp-таргет дага, старое имя этого таргета, флаг наличия переименования, зависимый даг на проде).

        :param filter: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        """
        dags_with_target_tables = self.package_analyzer.get_dags_with_target_tables(filter)
        targets_with_old_names = dict()
        targets_with_depend_dags = dict()

        for dag in dags_with_target_tables.keys():
            target_tables = dags_with_target_tables[dag]
            for target in target_tables:
                target_old_name = self.package_analyzer.get_name_of_table_before_renaming(target, 'GpTable')
                if target_old_name:
                    targets_with_old_names[target] = target_old_name
                    targets_with_depend_dags[target] = self.package_analyzer.get_dags_depend_on_table(target_old_name)

        params = []
        ids = []

        for target in targets_with_old_names.keys():
            for depend_dag in targets_with_depend_dags[target]:
                target_old_name = targets_with_old_names.get(target)
                if target_old_name:
                    rename_flg = (target != target_old_name)
                    params.append([target, target_old_name, rename_flg, depend_dag])
                    ids.append(', '.join([target, target_old_name, str(rename_flg), depend_dag]))

        self.__collect_local_params(params, ids, 'tedi_gp_target, target_old_name, rename_flag, depend_tedi_dag')

    def _param_dlh_target_and_old_name_and_depend_dag(self, filter: str = 'Dag{}') -> None:
        """Создам набор параметров.

         Набор параметров следующий:
         (dlh-таргет дага, старое имя этого таргета, флаг наличия переименования, зависимый даг на проде).

        :param filter: фильтр для дагов из пакета (по умолчанию 'Dag{}')
        """
        dags_with_target_tables = self.package_analyzer.get_dags_with_dlh_target_tables(filter)
        targets_with_old_names = dict()
        targets_with_depend_dags = dict()

        for dag in dags_with_target_tables.keys():
            target_tables = dags_with_target_tables[dag]
            for target in target_tables:
                target_old_name = self.package_analyzer.get_name_of_table_before_renaming(target, 'DlhTable')
                if target_old_name:
                    targets_with_old_names[target] = target_old_name
                    targets_with_depend_dags[target] = self.package_analyzer.get_dags_depend_on_table(target_old_name)

        params = []
        ids = []

        for target in targets_with_old_names.keys():
            for depend_dag in targets_with_depend_dags[target]:
                target_old_name = targets_with_old_names.get(target)
                if target_old_name:
                    rename_flg = (target != target_old_name)
                    params.append([target, target_old_name, rename_flg, depend_dag])
                    ids.append(', '.join([target, target_old_name, str(rename_flg), depend_dag]))

        self.__collect_local_params(params, ids, 'tedi_dlh_target, target_old_name, rename_flag, depend_tedi_dag')

    def filter_params(self, nums_of_params: Union[int, str] = 1, func_to_filter: Optional[Callable] = None):
        """Filter params by funcs give as parameters."""
        if isinstance(nums_of_params, str):
            nums_of_params = int(nums_of_params)
        list_to_del = [id for id, param in self.params if func_to_filter(param[nums_of_params - 1])]
        for id in list_to_del:
            self.params.filter_params(id, True)

    def add_custom_param(self, name: str, func_to_produce: Callable, nums_of_param_to_build: str = '1,2'):
        """Add custom params to exist or create param."""
        index_clear = nums_of_param_to_build.replace(' ', '').split(',')
        if index_clear != ['0']:
            new_name: str = '{0}, {1}'.format(self.params.names, name)
            new_params: List = []
            new_id: List[Any] = []
            for id, param in self.params:
                custom_id, custom_param = func_to_produce(*[param[int(i) - 1] for i in index_clear])
                for id_cust, custom_param in zip(custom_id, custom_param):
                    new_params.append(param + [custom_param])
                    new_id.append('{0},{1}'.format(id, id_cust))

            self.__collect_local_params(new_params, new_id, new_name)
        else:
            new_name = name
            custom_id, custom_param = func_to_produce()
            self.__collect_local_params(custom_param, custom_id, new_name)

    @abstractmethod
    def build(self):
        pass

    def return_params(self) -> Tuple[str, List[Any], List[str]]:
        params: List[Any] = []
        ids: List[str] = []
        for id, param in self.params:
            if param and len(param) == 1:
                params.append(param[0])
            else:
                params.append(param)
            ids.append(id)
        return self.params.names, params, ids
