import re
import json
import ast
import datetime
import pytz
import dateutil.parser
from collections import OrderedDict, defaultdict
from typing import List, Optional, Match, Dict, Set, Tuple, Union, Any

from Framework.ETLobjects.package_object import PackageObject
from Framework.ETLobjects.table import TableDetail
from Framework.ETLobjects.chimera_files import ChimeraFile
from Framework.utils import SASNameTransfer
from Framework.utils.chimera_util import ChimeraApi
from Framework.pytest_custom.CustomExceptions import NoScenarion


class deploy_parser:
    table_with_alias_regexp = r"\s+[as]*\s*(\w*)"
    any_table_regexp = r"\s+(&\w+..\w+)\s+[as]*\s*(\w*)"
    column_in_transform_regexp = r"%let\s+_OUTPUT0_col(\d{1,4})_input\d{1,3}\s+=\s+(\w+);"

    rename_reg_1 = r"%let\s+_OUTPUT0_col"
    rename_reg_2 = r"_exp\s+=\s+rename\(\w+,\s*"
    rename_reg_3 = r"\);"

    @staticmethod
    def get_table_alias_from_code(table: TableDetail, code: str) -> List[str]:
        """
        :param table: таблица
        :param code: кусок кода, где возможно есть таблица
        :return: списк alias для переданной таблицы в переданном коде
        """

        table_with_alias = re.findall(
            table.phys_name.replace(table.schema, table.meta_schema).lower() + deploy_parser.table_with_alias_regexp,
            code)

        return [alias.split(' ')[-1] for alias in table_with_alias]

    @staticmethod
    def get_all_tables_and_allias(code: str) -> List[Match]:
        """
        Находит все Match обращения к таблицам в переданном коде
        :param code: SAS или SQL код
        :return: список совпадений по шаблону
        """

        table_with_alias = re.findall(deploy_parser.any_table_regexp, code)

        return table_with_alias

    @staticmethod
    def get_column_without_alias_in_group_by(table, code, object_type: str = 'Table') -> list:
        """
        Метод для получения колонок из конструкции 'group by' без алиасов
        :param table: таблица для получения кода из SAS, в дагах не используется
        :param code: sql код в котором ищем
        :param object_type: тип объекта из конфига ТОЛЬКО 'Table' или 'GpTable'
        :return: список колонок без алиасов
        """
        list_of_columns_without_alias = []
        query = []

        if object_type == 'Table':
            if 'proc datasets lib = gp_wrk' in code:
                if '%etls_preparetarget;' in code:
                    query_tmp = code.split('%etls_preparetarget;')[1].split('%rset(&sqlrc);')[0]
                    query = query_tmp.split('select')[1:]
                elif 'proc datasets lib =' in code:
                    query_tmp = code.split('proc datasets lib =')[1].split('%rset(&sqlrc);')[0]
                    query = query_tmp.split('select')[1:]
        elif object_type == 'GpTable':
            query = code.lower().split('select')[1:]
        else:
            return []

        for select in query:
            if 'group by' in select:
                group_by_clause_tmp = select.split('group by')[1]
                if object_type == 'Table' and deploy_parser.get_table_alias_from_code(table, select):
                    group_by_clause_tmp = group_by_clause_tmp.split('by odbc;')[0]
                group_by_clause = group_by_clause_tmp.split('having')[0]
                splited_group_by = group_by_clause.split('\n')

                for row in splited_group_by:
                    if row.strip() != '' and '.' not in row:
                        column_name = row.replace(',', '').strip()

                        if re.sub(r'\d', '', row.replace(',', '').strip()) != '':
                            if not column_name.startswith(')'):
                                list_of_columns_without_alias.append(column_name)

            return list_of_columns_without_alias

        else:
            return []

    #ToDO перенесено из основного кода, можно оптимизировать, переделать
    @staticmethod
    def find_columns_in_code(list_of_psevdonims, code):
        list_of_check_columns = []
        splited_query = code.split('\n')
        for p in list_of_psevdonims:
            for row in splited_query:

                if p + '.' in row and 'schema..' not in row:
                    splited_row = row.split(' ')

                    for element in splited_row:
                        # Удаление символов ,)(:=><*+/ из названия столбца.
                        cleaned_element = re.sub('[,)(:=><*+/|]', '\t', element)
                        splited_cleaned_element = cleaned_element.split('\t')

                        for split_element in splited_cleaned_element:
                            if p + '.' in split_element and split_element.split('.')[0] == p:
                                column_name_tmp = split_element.replace('by odbc;', '').split(p + '.')[1].split(' ')[0]
                                column_name = column_name_tmp.strip()

                                if column_name in list_of_check_columns:
                                    continue

                                list_of_check_columns.append(column_name)

        return list_of_check_columns

    @staticmethod
    def find_all_columns_in_all_code(list_of_psevdonims, all_transforms):
        list_of_all_columns = []
        for transform in all_transforms:
            transform_code = transform.lower()

            if 'proc datasets lib = gp_wrk' in transform_code:
                # Для таблиц
                if '%etls_preparetarget;' in transform_code:
                    main_code = transform_code.split('%etls_preparetarget;')[1].split('%rcset(&sqlrc);')[0]
                # Для вьюх
                else:
                    main_code = transform_code.split('proc datasets lib =')[1].split('%rcset(&sqlrc);')[0]

                list_of_all_check_columns = deploy_parser.find_columns_in_code(list_of_psevdonims, main_code)

                list_of_all_columns.extend(list_of_all_check_columns)

        return list(set(list_of_all_columns))

    @staticmethod
    def find_is_notnull_in_where(list_of_psevdonims, list_of_check_columns_in_select, where_clause):
        list_of_columns_with_notnull_in_where = []
        for p in list_of_psevdonims:
            for column in list_of_check_columns_in_select:
                if p + '.' + column + ' is not null' in where_clause or p + '.' + column + ' notnull' in where_clause:

                    if column in list_of_columns_with_notnull_in_where:
                        continue

                    list_of_columns_with_notnull_in_where.append(column)

        return list_of_columns_with_notnull_in_where

    @staticmethod
    def get_use_columns_in_versioned_transform(code):
        all = re.findall(deploy_parser.column_in_transform_regexp, code)
        colums = set(all)
        return colums

    @staticmethod
    def get_rename_code_for_col(code, col_num, column):
        reg = deploy_parser.rename_reg_1 + col_num + deploy_parser.rename_reg_2 + column + deploy_parser.rename_reg_3
        code = code.replace(' );', ');')
        usage = re.findall(reg, code)

        return usage


class log_parser:
    table_and_depend_job_regexp = r"TARGET:\s+(?P<table>.+)\s+DEPEND_JOB_NAME:\s+(?P<job>.+)"

    @staticmethod
    def get_table_and_depend_job_from_log(log_text: str,
                                          exclude_jobs: Optional[List[PackageObject]] = None) -> Dict[str, List[str]]:
        """
        Возвращает словарь с таблицами и зависимымыми джобами
        :param log_text: текст лога из макроса по определению зависимых джобов
        :param exclude_jobs: список джобов, которые нужно исключить из выдачи
        :return Пример:
                        {
                         'GP_VAULT.S_APPLICATION_ILOG_CHNG': ['DDS LOAD SOME', 'EMART LOAD APPLICATIONS'],
                         'GP_OOD.DECISION_ARCH': ['DDS LOAD SOME', 'EMART LOAD APPLICATIONS']
                        }
        """
        find_item = [(match.group('table'), match.group('job')) for match in
                     re.finditer(log_parser.table_and_depend_job_regexp, log_text)]

        table_with_dep_jobs: OrderedDict[str, List[str]] = OrderedDict()

        for item in find_item:
            if exclude_jobs and item[1] in exclude_jobs:
                continue
            try:
                table_with_dep_jobs[item[0]].append(item[1])
            except KeyError:
                table_with_dep_jobs[item[0]] = []
                table_with_dep_jobs[item[0]].append(item[1])

        return table_with_dep_jobs


class SasScriptsParser:
    trunc_regex = r'(?i)truncate[\s\w]+&(\w.+)\s*;'
    load_params_set_regexp = r'(?i)mDWHSetLoadParam\((.+),(.+),(.+)'
    insert_regexp = r'(?i)insert\s+into\s+&(\w+..\w+)\s*(select \* from)?'
    all_modified_tables_regexp = r'(?i)execute[\s\d\w(;]+([&\w\d.]+)(\s*rename\s*to\s*)*([\d\w]*)'
    drop_regexp = r'(?i)drop\s+[\w\s]*\s+&(\w+..\w+)\s*(cascade)?'
    create_view_regexp = r'(?i)create\s+or\s+replace\s+view\s+&(\w+..\w+)\s+\w*([\s\S]*by &DWH_ENGINE_ALIAS.;)'
    create_table_regexp = r'(?i)create\s+table\s+&(\w+..\w+)\s+\w*([\s\S]*by &DWH_ENGINE_ALIAS.;)'
    drop_column_regexp = r'(?i)ALTER\s+TABLE\s+&(\w+..\w+)\s+DROP\s+COLUMN\s+\w+\s*(CASCADE)?'
    # набор паттернов на все UDD-функции
    # выбираем для проверки только поля *_code,
    # подробнее о функциях: https://wiki.tcsbank.ru/pages/viewpage.action?pageId=3214955290
    pattern_code_in_udd1 = r"udd_modify_dictionary\('([^']+)'"
    pattern_code_in_udd2 = r"udd_remove_dictionary\('([^']+)'"
    pattern_code_in_udd3 = r"udd_modify_dictionary_element\('([^']+)'"
    pattern_code_in_udd4 = r"udd_remove_dictionary_element\('([^']+)'"
    pattern_code_in_udd5 = r"udd_modify_dictionary_link\('([^']+)', '[^']+', '([^']+)'"
    pattern_code_in_udd6 = r"udd_remove_dictionary_link\('([^']+)', '([^']+)'"
    pattern_code_in_udd7 = r"udd_modify_blank_substitute\('([^']+)', '([^']+)'"
    pattern_code_in_udd8 = r"udd_remove_blank_substitute\('([^']+)', '([^']+)'"
    pattern_code_in_udd9 = r"udd_modify_absent_substitute\('([^']+)', '([^']+)'"
    pattern_code_in_udd10 = r"udd_remove_absent_substitute\('([^']+)', '([^']+)'"
    obsolete_table_regexp = r'(?i)ALTER\s+TABLE\s+&(\w+\.\.\w+)\s+SET\s+SCHEMA\s+&DWH_GP_OBS_SCHEMA'

    @classmethod
    def _open_script_code(cls, script_path):
        with open(script_path, errors='ignore') as f:
            code = f.read()
            return code

    @classmethod
    def find_all_truncate_tables_from_script(cls, script_path):
        code = cls._open_script_code(script_path)
        truncate_tables = {match.group(1) for match in re.finditer(cls.trunc_regex, code)}
        return truncate_tables

    @classmethod
    def find_all_truncate_tables_from_list_of_scripts(cls, list_of_scripts):
        set_of_truncate = set()
        for script_path in list_of_scripts:
            set_of_truncate = set_of_truncate | cls.find_all_truncate_tables_from_script(script_path)

        return set_of_truncate

    @classmethod
    def find_all_load_params_set_from_script(cls, script_path):
        code = cls._open_script_code(script_path)
        load_params_sets = {(match.group(1), match.group(2)) for match in re.finditer(cls.load_params_set_regexp, code)}
        return load_params_sets

    @classmethod
    def find_all_load_params_set_from_list_of_scripts(cls, list_of_scripts):
        set_of_load_params = set()
        for script_path in list_of_scripts:
            set_of_load_params = set_of_load_params | cls.find_all_load_params_set_from_script(script_path)

        return set_of_load_params

    @classmethod
    def find_all_table_with_insert_from_script(cls, script_path) -> Set[Tuple[str, Optional[str], str]]:
        code = cls._open_script_code(script_path)
        tables_with_insert = {(match.group(1), match.group(2), cls._get_script_name(script_path))
                              for match in re.finditer(cls.insert_regexp, code)}

        return tables_with_insert

    @classmethod
    def find_all_table_with_insert_from_list_of_scripts(cls, list_of_scripts):
        tables_with_insert = set()
        for script_path in list_of_scripts:
            tables_with_insert = tables_with_insert | cls.find_all_table_with_insert_from_script(script_path)
        return tables_with_insert

    @classmethod
    def find_all_modified_tables_from_script(cls, script_path: str, prefix: str):
        code = cls._open_script_code(script_path)
        macro_tables = set()
        for match in re.finditer(cls.all_modified_tables_regexp, code):
            if match.group(3):
                macro_schema = match.group(1).upper().split('..')[0]
                macro_tables.add(macro_schema + '..' + match.group(3).upper())
            else:
                macro_tables.add(match.group(1).upper())
        modified_tables = set()
        for table in macro_tables:
            if '..' in table:
                schema, name = table.replace('&', '').split('..')
                schema_gp, map_meta = SASNameTransfer.get_all_schems_name_by_macros_name(schema)
                # Костыль так как в файле есть не все схемы
                if schema_gp is None:
                    schema_gp = schema.replace('DWH_GP', 'test').replace('_SCHEMA', '')
                if '_ods_' in schema_gp:
                    phys_table = (table, schema_gp.lower() + '.' + name.lower())
                else:
                    phys_table = (table, schema_gp.lower().replace('test', prefix) + '.' + name.lower())
                modified_tables.add(phys_table)
        return modified_tables

    @classmethod
    def find_all_modified_tables_from_list_of_scripts(cls, list_of_scripts: list, prefix: str):
        modified_tables = set()
        for script_path in list_of_scripts:
            modified_tables = modified_tables | cls.find_all_modified_tables_from_script(script_path, prefix)

        return modified_tables

    @classmethod
    def find_all_drop_tables_from_script(cls, script_path) -> Set[Tuple[str, Optional[str], str]]:
        code = cls._open_script_code(script_path)
        tables_with_drop = {(match.group(1), match.group(2), script_path.rpartition('/')[2])
                            for match in re.finditer(cls.drop_regexp, code)}
        return tables_with_drop

    @classmethod
    def find_all_drop_tables_from_list_of_scripts(cls, list_of_scripts):
        tables_with_drop = set()
        for script_path in list_of_scripts:
            tables_with_drop = tables_with_drop | cls.find_all_drop_tables_from_script(script_path)
        return tables_with_drop

    @classmethod
    def _get_script_name(cls, script):
        return script.rpartition('/')[2]

    @classmethod
    def find_all_drop_columns_from_script(cls, script_path) -> Set[Tuple[str, Optional[str], str]]:
        code = cls._open_script_code(script_path)
        columns_with_drop = {(match.group(1), match.group(2), cls._get_script_name(script_path))
                             for match in re.finditer(cls.drop_column_regexp, code)}
        return columns_with_drop

    @classmethod
    def find_all_drop_columns_from_list_of_scripts(cls, list_of_scripts):
        columns_with_drop = set()
        for script_path in list_of_scripts:
            columns_with_drop = columns_with_drop | cls.find_all_drop_columns_from_script(script_path)
        return columns_with_drop

    @classmethod
    def find_all_table_with_create_view_from_script(cls, script_path) -> Set[Tuple[str, str, str]]:
        code = cls._open_script_code(script_path)
        all_views = {(match.group(1), match.group(2), cls._get_script_name(script_path))
                     for match in re.finditer(cls.create_view_regexp, code)}

        return all_views

    @classmethod
    def find_all_table_with_create_view_list_of_scripts(cls, list_of_scripts):
        all_views = set()
        for script_path in list_of_scripts:
            all_views = all_views | cls.find_all_table_with_create_view_from_script(script_path)

        return all_views

    @classmethod
    def find_all_table_with_create_table_script(cls, script_path) -> Set[Tuple[str, Optional[str], str]]:
        code = cls._open_script_code(script_path)
        create_tables = {(match.group(1), match.group(2), cls._get_script_name(script_path))
                            for match in re.finditer(cls.create_table_regexp, code)}

        return create_tables

    @classmethod
    def find_all_table_with_create_table_list_of_scripts(cls, list_of_scripts):
        create_tables = set()
        for script_path in list_of_scripts:
            create_tables = create_tables | cls.find_all_table_with_create_table_script(script_path)
        return create_tables

    @classmethod
    def find_all_obsolete_tables_from_script(cls, script_path):
        code = cls._open_script_code(script_path)
        return re.findall(cls.obsolete_table_regexp, code)


class ChimeraSqlScriptParser(SasScriptsParser):
    trunc_regex = r'(?i)truncate[\s\w]+([<>_]*\w.+)\s*;'
    insert_regexp = r'(?i)INSERT\s+INTO\s+([<>_]*\w+\.\w+)\s+[(]*SELECT (.+?) FROM'
    all_modified_tables_regexp = '([\w,]*)\s+(<>_\w+\.\w+)[\s;]'
    drop_regexp = r'(?i)drop\s+[table|view][\w\s]*\s+([<>_]*\w+.\w+)\s*(cascade)?'
    create_view_regexp = r'(?i)(create)\s+or\s+replace\s+view\s+([<>_]*\w+.\w+)\s+'
    create_table_regexp = r'(?i)create\s+table\s+([<>_]*\w+.\w+)\s+'
    drop_column_regexp = r'(?i)ALTER\s+TABLE\s+([<>_]*\w+.\w+)\s+DROP\s+COLUMN\s+\w+\s*(CASCADE)?'
    drop_id_translate = r'(?i)DROP\s+TABLE[\w\s]*\s+([<>_]*_utl_md.id_translate_\w+)[\s+|;]'

    @classmethod
    def _open_script_code(cls, chimera_script_file):
        code = chimera_script_file.get_content()
        return code

    @staticmethod
    def delete_comments_in_script(code: str) -> str:
        """
        Удаляет комментарии из скрипта
        Parameters
        ----------
        code: str
            код скрипта

        Returns
        -------
        code_without_comments: str
        """
        pattern = r'\/\*[.\s\S]*?\*\/'
        code = re.sub(pattern, '', code)
        lines = code.split("\n")
        modified_lines = []
        for line in lines:
            if '--' in line:
                start_index = line.index('--')
                modified_line = line[:start_index]
                modified_lines.append(modified_line)
            else:
                modified_lines.append(line)
        code_without_comments = "\n".join(modified_lines)
        return code_without_comments


    @classmethod
    def _get_script_name(cls, script):
        return script.name

    @classmethod
    def find_all_table_with_insert_from_script(cls, script) -> Set[Tuple[str, Optional[str], str]]:
        code = cls._open_script_code(script)
        tables_with_insert = {(match.group(1), ('*' in match.group(2)), script.name)
                              for match in re.finditer(cls.insert_regexp, code)}

        return tables_with_insert

    @classmethod
    def find_all_droped_idtr_from_script(cls, script) -> Set[str]:
        code = cls._open_script_code(script)
        code = cls.delete_comments_in_script(code)
        droped_tables = {match.group(1) for match in re.finditer(cls.drop_id_translate, code)}

        return droped_tables

    @classmethod
    def find_all_droped_idtr_from_list_of_scripts(cls, list_of_scripts: list) -> Set[str]:
        droped_tables = set()
        for script in list_of_scripts:
            droped_tables = droped_tables | cls.find_all_droped_idtr_from_script(script)

        return droped_tables

    @classmethod
    def find_all_modified_tables_from_chimera_script(cls, chimera_script_file: ChimeraFile):
        code = cls._open_script_code(chimera_script_file)
        code = cls.delete_comments_in_script(code)
        modified_tables = set()
        for match in re.finditer(cls.all_modified_tables_regexp, code):
            # Если перед таблицей имеются from или join, то считаем такие таблицы как источники (а не модифицирующими)
            if match.group(1).upper() not in ('FROM', 'JOIN'):
                # Исторически метод возвращает таблицы с префиксом test_
                # Приводим к нижнему регистру в соответсвии с конфигами Химеры
                table_phys = match.group(2).replace('<>_', 'test_').lower()
                modified_tables.add(table_phys)
        return modified_tables

    @classmethod
    def find_all_modified_tables_from_list_of_scripts(cls, list_of_scripts: list, prefix: str = None):
        modified_tables = set()
        for script in list_of_scripts:
            modified_tables = modified_tables | cls.find_all_modified_tables_from_chimera_script(script)

        return modified_tables

    @classmethod
    def find_all_udd_function_from_list_of_scripts(cls, list_of_scripts):
        code_with_udd = set()
        for script_path in list_of_scripts:
            code_with_udd = code_with_udd | cls.find_all_udd_function_from_script(script_path)
        return code_with_udd

    @classmethod
    def find_all_udd_function_from_script(cls, script_path) -> Set[Tuple[str, Optional[str], str]]:
        def get_codes_in_findall(findall_results: List[Union[str, Tuple[str]]]) -> Set[str]:
            """Функция приводит результат регулярки к "общему знаменателю" - множество ключей из UDD-функций."""
            if len(findall_results) > 1:
                # Без этой проверки каждая строка в массиве распилится посимвольно
                if isinstance(findall_results[0], str):
                    return {*findall_results}
                return {item for findall_result in findall_results for item in findall_result}
            elif findall_results:
                # Без этой проверки строка распилится посимвольно
                if isinstance(findall_results[0], str):
                    return {*findall_results}
                return {*findall_results[0]}
            else:
                return set()

        code = cls._open_script_code(script_path)
        code_with_udd_1 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd1, code))
        code_with_udd_2 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd2, code))
        code_with_udd_3 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd3, code))
        code_with_udd_4 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd4, code))
        code_with_udd_5 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd5, code))
        code_with_udd_6 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd6, code))
        code_with_udd_7 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd7, code))
        code_with_udd_8 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd8, code))
        code_with_udd_9 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd9, code))
        code_with_udd_10 = get_codes_in_findall(re.findall(cls.pattern_code_in_udd10, code))
        code_with_udd = set()
        code_with_udd.update(code_with_udd_1, code_with_udd_2, code_with_udd_3, code_with_udd_4, code_with_udd_5,
                             code_with_udd_6, code_with_udd_7, code_with_udd_8, code_with_udd_9, code_with_udd_10)
        return code_with_udd

    @classmethod
    def find_all_schemas_in_scripts(cls, list_of_scripts: List[str]) -> Dict[str, List[str]]:
        """Найти все схемы таблиц, встречающиеся в каждом скрипте в Chimera"""
        all_schemas = defaultdict(list)
        for script in list_of_scripts:
            code = cls.delete_comments_in_script(cls._open_script_code(script))
            schemas_in_scripts = [full_table_name.split('.')[0]
                                  for _, full_table_name in re.findall(cls.all_modified_tables_regexp, code)]
            for schema_name in schemas_in_scripts:
                name_without_prefix = schema_name.replace('<>', '').lower()
                all_schemas[script.name].append(name_without_prefix)
        return all_schemas


class ScenarioParser:
    use_chimera = 'no'
    task = ''

    @classmethod
    def _open_scenario_code(cls, package_scenario_path):
        if cls.use_chimera == 'no':
            with open(package_scenario_path, 'r') as f:
                scenario_code = json.load(f)
                return scenario_code
        else:
            scenario = ChimeraApi().get_package_scenario(cls.task)
            scenario_json_data = scenario

            custom_format_for_old_tests = OrderedDict()
            for step in scenario_json_data:
                custom_format_for_old_tests[step['step_index'] + 1] = step
                custom_format_for_old_tests[step['step_index'] + 1]['name'] = \
                custom_format_for_old_tests[step['step_index'] + 1]['step_type']

                for param in custom_format_for_old_tests[step['step_index'] + 1]['parameters']:
                    if param['name'] == 'object_names':
                        custom_format_for_old_tests[step['step_index'] + 1]['objects'] = param['value']
                        if param.get('allowed_values'):
                            allowed_val = [val['value'] for val in param.get('allowed_values')]
                            custom_format_for_old_tests[step['step_index'] + 1]['allowed_values'] = allowed_val

                scenario_json_data = custom_format_for_old_tests

            return scenario_json_data

    @classmethod
    def get_scenario_steps(cls, package_scenario_path):
        scenario_code = cls._open_scenario_code(package_scenario_path)
        scenario_list = []
        steps = []
        if not scenario_code:
            raise NoScenarion("Не можем найти сформированный сценарий наката в Chimera, сделайте Build")
        for key, value in scenario_code.items():
            scenario_list.append(value)

        for element in scenario_list:
            steps.append(element['name'])

        return steps


class CHSqlScriptsParser(ChimeraSqlScriptParser):
    create_with_field_regex = r'(?i)CREATE\s+TABLE\s+[IF\sNOT\sEXISTS\s]*\s(.+\.\w+)\s+' \
                              r'[ON\sCLUSTER\w]*\s*\(([\w\s,\(\)`\'"]+)\)\s*ENGINE\s*=\s*([^)]+\))'
    pk_section_regex = r'(?i)PRIMARY\s+KEY\s*(\(?[\)\w\s,]+)(PARTITION|ORDER|TTL|SAMPLE|SETTINGS|;)+'
    order_by_section_regex = r'(?i)ORDER\s+BY\s*(\(?[\w\s,\)]+)(PARTITION|PRIMARY|TTL|SAMPLE|SETTINGS|;)+'

    @classmethod
    def _read_script_code(cls, script_path: str):
        with open(script_path, errors='ignore') as f:
            code = f.read()
            return code

    @classmethod
    def find_tables_created_in_script(cls, script: ChimeraFile):
        creating_ch_tables = []
        code = cls._open_script_code(script)
        code = cls.delete_comments_in_script(code)
        for match in re.finditer(cls.create_with_field_regex, code):
            if match.group(1):
                ch_table_details_dict = {}
                t_name = match.group(1)
                ch_table_details_dict[t_name] = {}
                ch_table_details_dict[t_name]['table'] = t_name
                ch_table_details_dict[t_name]['fields_block'] = match.group(2)
                ch_table_details_dict[t_name]['engine_block'] = match.group(3)
                opt_section = code[match.end():].strip()
                if opt_section:
                    pk_section = cls.get_primary_key_section(opt_section)
                    ch_table_details_dict[t_name]['pk_block'] = pk_section
                    ob_section = cls.get_order_by_section(opt_section)
                    ch_table_details_dict[t_name]['ordby_block'] = ob_section
                creating_ch_tables.append(ch_table_details_dict)
        return creating_ch_tables

    @classmethod
    def get_primary_key_section(cls, opt_section: str) -> str:
        pk_sec = ''
        match = re.search(cls.pk_section_regex, opt_section)
        if match and match.group(1):
            pk_sec = str.strip(match.group(1))
        return pk_sec

    @classmethod
    def get_order_by_section(cls, opt_section: str) -> str:
        ob_sec = ''
        match = re.search(cls.order_by_section_regex, opt_section)
        if match and match.group(1):
            ob_sec = str.strip(match.group(1))
        return ob_sec


class ReviewCutParser:
    CUT_TABLES_PARAM_FIELDS = ('key_tables', 'tables')
    CUT_QUERY_PARAM_FIELD = 'insert_query'

    @classmethod
    def parse_cut(cls, s: str) -> Dict[str, Any]:
        """Находит и парсит отдельное сообщение, содержащее параметры cut2.
        Сообщение является repr-ом python-объекта. Помимо строк, чисел в нем
        содержаться объекты datetime. Перед тем, как делать eval (ast.literal_eval)
        всего сообщения, найденные datetime переводятся в isoformat
            Пример искомого сообщения в логе:

            >2021-10-28 13:49:58,883|INFO|cut2.py:158|{'finish_dttm': datetime.datetime(2021, 10, 28, 16, 49, 57, 747179),
                'insert_dttm': datetime.datetime(2021, 10, 28, 16, 48, 13, 296546),
                'key_tables': [{'build_time': '0:00:11.911728',
                                'insert_query': 'insert into '
            ...
        """

        def _dttmrepl(matchobj):
            group = matchobj.group(0)
            if group:
                res = eval(group, {'datetime': datetime})
                return f"'{str(res)}'"

        body = {}
        insert_queries = []
        cut2_module_name_rgx = re.compile(r'(?im)^cut2.*\.py')
        datetime_rgx = re.compile(r'(datetime.datetime\([\d\s,].*\))')
        response_rgx = re.compile(r'(?im)^response.*\{')
        try:
            for line in s.split('\n>'):
                line_lst = line.split('|')
                if len(line_lst) != 4:
                    continue
                *_, file, msg = line_lst
                if cut2_module_name_rgx.match(file) and cls.CUT_QUERY_PARAM_FIELD in msg:
                    body = datetime_rgx.sub(_dttmrepl, msg)
                    body = response_rgx.sub('{', body)
                    body = ast.literal_eval(body)
                    break

            for key, value in body.items():
                if key not in cls.CUT_TABLES_PARAM_FIELDS:
                    continue
                if cls.CUT_QUERY_PARAM_FIELD in value:
                    insert_queries.append({cls.CUT_QUERY_PARAM_FIELD: value[cls.CUT_QUERY_PARAM_FIELD]})
        except Exception as e:
            pass
        return {'cut': body, '_insert_queries': insert_queries}



class TediDagLogParser:
    """Класс для парсинга плоского текста логов запуска DAGа"""
    @classmethod
    def parse_header(cls, s: str) -> Dict[str, str]:
        """Парсинг настроек блока ноды DAGа, на вход идет изолированный кусок лога задачи"""
        header, *_, body = re.split(r"#=+#\r?\n", s)
        if "(403)\nReason: Forbidden" in body or '[Errno -3] Temporary failure' in body:
            header, *_, body, tech_body = re.split(r"#=+#\r?\n", s)
        def _convert_to_local_tz(s: str, tz: datetime.tzinfo) -> str:
            return dateutil.parser \
                .parse(s) \
                .replace(tzinfo=pytz.utc) \
                .astimezone(tz) \
                .isoformat()

        res = {}
        for line in header.splitlines():
            left, right = line.split(':', 1)
            left = left.strip('# ')
            right = right.strip('# ')
            res[left] = right
        required_fields = {'start_date', 'end_date', 'task_id', 'duration', 'state'}
        assert required_fields <= res.keys()

        # Дефолтная зона в airflow сейчас - UTC
        # (https://gitlab.tcsbank.ru/tedi/tedi-core/-/blob/master/docker/airflow.cfg)
        # Отчет по логам строится в локальной зоне
        local_timezone = pytz.timezone('Europe/Moscow')
        res['start_date'] = _convert_to_local_tz(res['start_date'], local_timezone)
        res['end_date'] = _convert_to_local_tz(res['end_date'], local_timezone)
        return res

    @classmethod
    def find_rows_info(cls, text: str) -> str:
        """Поиск блока с обработкой строк, подается любой текст и отдается последний результат"""
        reg_loader = r'(?i)\"table_cnt\"\:\s+([0-9]+)\,'
        reg = r'Processed\s+([0-9]+)\s+rows'
        res = ''
        matches = re.finditer(reg_loader, text)
        for match in matches:
            res = match.group(1)
        
        if not res:
            matches = re.finditer(reg, text)
            for match in matches:
                res = match.group(1)
        return res

    @classmethod
    def parse_status(cls, log_text: str) -> Dict[str, Any]:
        """Находим финальный статус во всем логе DAGа"""
        _, data, s = cls.split_raw_to_base_parts(log_text)
        pattern = r'(?<=job_status\s-\s).*'
        matched = re.findall(pattern, s.lower())
        res = {}
        if len(matched) == 1:
            res['status'] = matched[0]
        return res

    @classmethod
    def split_raw_to_base_parts(cls, s: str) -> List[str]:
        """"Базовая разбивка по блокам через разделитель"""
        pattern = r"(?<!=)======================\r?\n"
        res = [x for x in re.split(pattern, s) if x]
        return res[:3]

    @classmethod
    def split_raw_to_tasks(cls, log_text: str) -> List[str]:
        """Разбиение лога на основные задачи"""
        _, s, _ = cls.split_raw_to_base_parts(log_text)
        pattern = r"INFO\s*\r?\n#=+#\r?\n"
        res = re.split(pattern, s)[1:]

        return res


class SqlParser:
    @staticmethod
    def get_all_table_aliases(sql_query: str) -> Dict[str, str]:
        """
        Извлекает все алиасы таблиц из SQL-запроса.
        Если у таблицы нет алиаса, возвращает полное имя таблицы (включая схему, если указана).

        :param sql_query: SQL-запрос

        :returns: Словарь {полное_имя_таблицы: алиас}
        """
        # Нормализуем SQL-запрос: удаляем лишние пробелы и переносы строк
        sql_normalized = ' '.join(sql_query.split())

        # Словарь для хранения результатов
        table_aliases = {}

        # Извлекаем все части FROM и JOIN
        from_clauses = re.findall(r'\bfrom\b\s+(.+?)(?:\bwhere\b|\bgroup by\b|\bhaving\b|\border by\b|\blimit\b|$)',
                                  sql_normalized, re.IGNORECASE)

        join_clauses = re.findall(
            r'\b((?:left|right|inner|outer|cross|full)?\s*join\b\s+.+?)\s+(?:\bon\b|\bwhere\b|\bjoin\b|\bgroup by\b|\bhaving\b|\border by\b|\blimit\b|$)',
            sql_normalized, re.IGNORECASE)

        # Обрабатываем все FROM-части
        for clause in from_clauses:
            # Разбиваем на отдельные таблицы (могут быть перечислены через запятую)
            tables_in_from = clause.split(',')

            for table_entry in tables_in_from:
                table_entry = table_entry.strip()

                # Пропускаем пустые записи
                if not table_entry:
                    continue

                # Извлекаем имя таблицы и алиас
                # Варианты: "table AS alias", "table alias", "schema.table AS alias"
                parts = re.split(r'\bas\b', table_entry, flags=re.IGNORECASE)

                if len(parts) > 1:
                    # Случай "table AS alias"
                    table_name = parts[0].strip()
                    alias_part = parts[1].strip()
                    # Берем только первое слово как алиас
                    alias = alias_part.split()[0] if alias_part.split() else table_name
                else:
                    # Случай "table alias" или просто "table"
                    parts = parts[0].strip().split()
                    if len(parts) > 1:
                        table_name = parts[0]
                        # Проверяем, что второе слово не является ключевым словом SQL
                        if parts[1].lower() not in ('on', 'where', 'group', 'having', 'order', 'limit', 'join'):
                            alias = parts[1]  # Второе слово - алиас
                        else:
                            alias = table_name  # Если нет алиаса, используем имя таблицы
                    else:
                        table_name = parts[0]
                        alias = table_name  # Если нет алиаса, используем имя таблицы

                # Очищаем имя таблицы от возможных скобок, кавычек и т.д.
                table_name = re.sub(r'["\[\]]', '', table_name)

                # Добавляем в результаты
                table_aliases[table_name] = alias

        # Обрабатываем все JOIN-части
        for clause in join_clauses:
            # Извлекаем имя таблицы и алиас из JOIN
            # Паттерн: JOIN [table] [AS] [alias] ON
            join_table_match = re.search(r'join\s+([^\s]+)\s+(?:as\s+)?([a-zA-Z0-9_]+)(?:\s+on|$)', clause,
                                         re.IGNORECASE)

            if join_table_match:
                table_name = join_table_match.group(1)
                alias = join_table_match.group(2)

                # Очищаем имя таблицы от возможных скобок, кавычек и т.д.
                table_name = re.sub(r'["\[\]]', '', table_name)

                # Добавляем в результаты
                table_aliases[table_name] = alias
            else:
                # Более сложный случай JOIN
                # Ищем таблицу в JOIN-выражении
                join_parts = clause.split()
                # Находим индекс слова JOIN
                join_index = -1
                for i, part in enumerate(join_parts):
                    if part.lower() == 'join':
                        join_index = i
                        break

                if join_index >= 0 and join_index + 1 < len(join_parts):
                    table_name = join_parts[join_index + 1]

                    # Очищаем имя таблицы от возможных скобок, кавычек и т.д.
                    table_name = re.sub(r'["\[\]]', '', table_name)

                    # Ищем алиас
                    if join_index + 2 < len(join_parts):
                        if join_parts[join_index + 2].lower() == 'as' and join_index + 3 < len(join_parts):
                            alias = join_parts[join_index + 3]
                        else:
                            # Проверяем, не является ли следующее слово ключевым
                            next_word = join_parts[join_index + 2].lower()
                            if next_word not in ('on', 'where', 'group', 'having', 'order', 'limit'):
                                alias = join_parts[join_index + 2]
                            else:
                                alias = table_name  # Если нет алиаса, используем имя таблицы

                        # Добавляем в результаты
                        table_aliases[table_name] = alias
                    else:
                        # Если нет алиаса, используем имя таблицы
                        table_aliases[table_name] = table_name

        return table_aliases

    @classmethod
    def extract_join_filters(cls, sql_query: str) -> List[str]:
        """
        Извлекает фильтры из условий JOIN в SQL-запросе.

        :param sql_query: SQL-запрос

        :returns: Список найденных фильтров
        """
        # Нормализуем SQL-запрос: удаляем лишние пробелы и переносы строк
        sql_normalized = ' '.join(sql_query.split())

        # Список для хранения найденных фильтров
        join_filters = []

        # Извлекаем все JOIN-части с условиями ON
        join_clauses = re.findall(
            r'\b(?:left|right|inner|outer|cross|full)?\s*join\b\s+.+?\s+\bon\b\s+(.+?)(?=\b(?:left|right|inner|outer|cross|full)?\s*join\b|\bwhere\b|\bgroup by\b|\bhaving\b|\border by\b|\blimit\b|$)',
            sql_normalized, re.IGNORECASE)

        # Обрабатываем каждое условие JOIN
        for on_condition in join_clauses:
            # Предварительно обрабатываем BETWEEN ... AND ... чтобы защитить AND внутри BETWEEN
            # Заменяем "BETWEEN X AND Y" на "BETWEEN X _AND_ Y"
            processed_condition = re.sub(
                r'\bBETWEEN\s+([^\'"\s]+|\'[^\']*\'|"[^"]*")\s+AND\s+([^\'"\s]+|\'[^\']*\'|"[^"]*")',
                r'BETWEEN \1 _AND_ \2',
                on_condition,
                flags=re.IGNORECASE
            )

            # Разбиваем условие на части, разделенные AND (но не _AND_)
            and_conditions = re.split(r'\bAND\b(?!\s*_)', processed_condition, flags=re.IGNORECASE)

            for condition in and_conditions:
                # Удаляем лишние пробелы и восстанавливаем _AND_ обратно в AND
                condition = condition.strip().replace('_AND_', 'AND')

                # Пропускаем пустые условия
                if not condition:
                    continue

                # Проверяем, является ли условие фильтром
                if cls.is_filter_condition(condition):
                    join_filters.append(condition)

        return join_filters

    @staticmethod
    def is_filter_condition(condition: str) -> bool:
        """
        Определяет, является ли условие фильтром.

        :param condition: Условие для проверки

        :returns: True, если условие является фильтром, иначе False
        """
        # Проверяем наличие операторов сравнения
        comparison_operators = ['=', '!=', '<>', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'BETWEEN',
                                'IS NULL', 'IS NOT NULL']

        # Проверяем, содержит ли условие оператор сравнения
        has_comparison = any(op.lower() in condition.lower() for op in comparison_operators)

        if not has_comparison:
            return False

        # Проверяем, является ли условие сравнением двух столбцов из разных таблиц
        # Паттерн: [table1].[column1] = [table2].[column2]
        table_column_comparison = re.search(r'(\w+)\.(\w+)\s*(?:=|!=|<>|>|<|>=|<=)\s*(\w+)\.(\w+)', condition)

        if table_column_comparison:
            # Если сравниваются столбцы из разных таблиц, это условие соединения, а не фильтр
            table1 = table_column_comparison.group(1)
            table2 = table_column_comparison.group(3)
            return table1 == table2  # Если таблицы одинаковые, это фильтр (самосоединение)

        # Проверяем наличие констант в условии
        has_string_constant = bool(re.search(r"'[^']*'", condition))  # Строковые константы в одинарных кавычках
        has_numeric_constant = bool(re.search(r'\b\d+(?:\.\d+)?\b', condition))  # Числовые константы

        # Если есть константы, это, скорее всего, фильтр
        if has_string_constant or has_numeric_constant:
            return True

        # Проверяем наличие функций без ссылок на другие таблицы
        has_function = bool(re.search(r'\w+\([^)]*\)', condition))

        # Проверяем условия IS NULL/IS NOT NULL
        is_null_condition = bool(re.search(r'\bIS\s+(?:NOT\s+)?NULL\b', condition, re.IGNORECASE))

        return has_function or is_null_condition
