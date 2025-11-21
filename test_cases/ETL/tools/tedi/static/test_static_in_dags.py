import yaml
import re
from datetime import datetime, timedelta
from difflib import ndiff
from collections import defaultdict
from copy import deepcopy

import pytest
import allure

from Framework.utils.ddl_compare import CompareDdl
from Framework.utils.consumers_util import CoreConsumersAPIClient
from helpers.common import set_prefix_in_schema


@pytest.mark.integration
@allure.id("1874585")
@allure.feature('Job (Dag)')
@allure.story('Хардкод')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'В нодах дага присутствует vial_prefix')
@allure.description('Проверяет наличие хардкода в DAGе')
def test_vial_schema_in_dag_code(package_dag_by_repo, vial_prefix) -> None:
    with allure.step('Get code of DAG'):
        transforms = package_dag_by_repo.get_transforms_code()
        transforms_with_bad_prefix = []
        for transform in transforms:
            with allure.step(f"Check transform: {transform.get('description')}"):
                code = str(transform)
                if vial_prefix in code:
                    transforms_with_bad_prefix.append(transform.get('task_id'))
        assert not transforms_with_bad_prefix, f'У дага {package_dag_by_repo.name} в нодах ' \
                                               f'{transforms_with_bad_prefix} есть недопустимый префикс ' \
                                               f'(вероятно хардкод со схемой)'


@pytest.mark.integration
@allure.id("5288803")
@allure.feature('Job (Dag)')
@allure.story('Хардкод')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: В нодах дага присутствует vial_prefix')
@allure.description('Проверяет наличие хардкода в DAGу')
def test_vial_schema_in_dag_code_for_dlh(package_dag_by_repo, vial_dlh_prefix) -> None:
    with allure.step('Get code of DAG'):
        transforms = package_dag_by_repo.get_transforms_code()
        transforms_with_bad_prefix = []
        for transform in transforms:
            with allure.step(f"Check transform: {transform.get('description')}"):
                code = str(transform)
                if vial_dlh_prefix in code:
                    transforms_with_bad_prefix.append(transform.get('task_id'))
        assert not transforms_with_bad_prefix, f'У дага {package_dag_by_repo.name} в нодах ' \
                                               f'{transforms_with_bad_prefix} есть недопустимый префикс ' \
                                               f'(вероятно хардкод со схемой)'


@pytest.mark.integration
@allure.id("5265649")
@allure.feature('Job (Dag)')
@allure.story('Код Дага')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'Разные типы данных в coalesce')
@allure.description('''
Проверяет есть ли несовпадение типов в полях, переданных в coalesce в нодах DAGа.

1. Принцип работы:
Тест проверяет все выражения вида:
- coalesce(field1, ... , fieldN)
- coalesce(field1::new_type, ... , fieldN::new_type)

Кроме того тест пробегает по всему lineage дага и следит за именением типа каждого поля, указанного в coalesce.

2. Проблемы:
Данный тест призван отлавливать различия типов у полей, которые перечислены внутри coalesce().
Но поскольку внутри coalesce может быть не просто перечисление полей, а еще и:
- приведения типов
- выражения CASE
- вложенные функции
- и т.д.
то на таких кейсах тест может ломаться.

Обнаруженные пользователями ошибки мы вынесли в задачу https://jira.tcsbank.ru/browse/DPQA-2261

3. Что делать если тест упал / сломался:
- Проверить вручную, что в вашем пакете нет дагов, у которых в coalesce указаны поля с разными типами
    - Если нет - просто игнорируем тест
    - Если есть - исправляем эту проблему
- Посмотреть на проблему с которой упал тест:
    - если она описана в https://jira.tcsbank.ru/browse/DPQA-2261 или в Известных проблемах - ничего не делаем
    - если она не указана нигде - заводим обращение в боте
''')
def test_different_types_in_coalesce_in_dag_code(package_dag, GP_integration, vial_prefix) -> None:
    # Получаем код DAG'а
    nodes = package_dag.get_transforms_code()

    # фильтруем ноды, оставляя только те, которые есть в TediGpTransforms
    from Framework.ETLobjects.dag import TediGpTransforms, enum_to_str_list
    sql_transforms_ids = enum_to_str_list(TediGpTransforms)
    nodes = [node for node in nodes if node['type'] in sql_transforms_ids]

    # Ищем в каких нодах есть coalesce
    if not any(list(map(lambda x: 'coalesce(' in x['params'].get('sql', ''), nodes))):
        pytest.skip("Coalesce are not exist in DAG's code")

    # Получаем lineage
    nodes_with_lineage = deepcopy(nodes)
    for node in nodes_with_lineage:
        # добавляем lineage в словарь с описанием для каждой ноды
        for lineage in package_dag.tedi_api.get_dag_lineage(package_dag.name)['lineages']:
            if lineage['task_id'] == node['task_id']:
                node['lineage'] = lineage

        # Формируем историю изменений типов полей по мере их движения по DAG"
        node.update({'col_types': {}})
        for column_name, parent_columns_list in node['lineage']['dependencies'].items():
            types_of_fields = defaultdict(dict)
            for parent_column in parent_columns_list:

                # поле тянется из ноды DAG'а, поэтому в названии поля указаны:
                # имя ноды откуда оно тянется и само название поля, разделенные точкой
                if parent_column.count('.') == 1:
                    task_id, parent_column_name = parent_column.split('.')

                    try:
                        node_with_lineage = [node for node in nodes_with_lineage if node['task_id'] == task_id]
                        field_types = list(node_with_lineage[0]['col_types'][parent_column_name].values())[0][parent_column_name]
                        types_of_fields[task_id].update({
                            parent_column_name: {
                                'old_type': (field_types['new_type'] if field_types['new_type'] else field_types['old_type']),
                                'new_type': None
                            }
                        })

                    # поле составное и формируется на основе нескольких полей предыдущих нод
                    except KeyError:
                        pre_types = list(
                            [node for node in nodes_with_lineage if node['task_id'] == task_id][0]['col_types'][
                                parent_column_name].values())
                        field_types = pre_types[0] if pre_types else []
                        new_types = {i['new_type'] for i in field_types.values()}

                        assert len(new_types) == 1, \
                            f"ERROR: Не удалось определить тип поля {parent_column_name} в ноде {task_id}"

                        types_of_fields[task_id].update({parent_column_name: {'old_type': new_types.pop(),
                                                                              'new_type': None}})

                    # поле создается в самом SELECT блоке запроса в предыдущих нодах
                    except IndexError:
                        types_of_fields[task_id].update({parent_column_name: {'old_type': 'CUSTOM FIELD',
                                                                              'new_type': 'CUSTOM FIELD'}})

                    # в SQL коде есть приведение типов для этого поля
                    # принудительно убираю все пробелы перед приведением типа ::
                    sql_code = re.sub(r'\s*::', '::', node['params'].get('sql', ''))

                    if f'{parent_column_name}::' in sql_code:
                        new_type = re.findall(f'{parent_column_name}::([\w\s(]*)[,\s*|),\s*|)\s*]',
                                              sql_code,
                                              re.IGNORECASE)[0]
                        if '(' in new_type:
                            # нужно прибавлять вручную ")", поскольку регулярка отсекает ")"
                            # из выражения с приведением типов
                            # если убрать из регулярки выше символ "(", то в приводимый тип
                            # будет захватываться еще и ") as"
                            new_type += ')'
                        types_of_fields[task_id][parent_column_name]['new_type'] = new_type

                # поле тянется из таблицы источника, поэтому в названии поля указаны:
                # схема, таблица и само название поля, разделенные точкой
                if parent_column.count('.') == 2:
                    schema, table, parent_column_name = parent_column.split('.')
                    full_name = f'{schema}.{table}'

                    for column_name_and_type in CompareDdl().get_columns_types_for_given_table(full_name):
                        if column_name_and_type['name'] == parent_column_name:
                            types_of_fields[full_name].update({
                                column_name_and_type['name']: {
                                    'old_type': column_name_and_type['type'],
                                    'new_type': None
                                }
                            })

                            # в SQL коде есть приведение типов для этого поля
                            # принудительно убираю все пробелы перед приведением типа ::
                            sql_code = node['params'].get('sql')
                            node_sql_code_without_inner_spaces = ''
                            if sql_code:
                                node_sql_code_without_inner_spaces = re.sub(r'\s*::', '::', sql_code)

                            if f'{parent_column_name}::' in node_sql_code_without_inner_spaces:
                                new_type = \
                                re.findall(f'{parent_column_name}::([\w\s(]*)[,\s*|),\s*|)\s*]',
                                           node_sql_code_without_inner_spaces,
                                           re.IGNORECASE)[0]
                                if '(' in new_type:
                                    # нужно прибавлять вручную ")", поскольку регулярка отсекает ")"
                                    # из выражения с приведением типов
                                    # если убрать из регулярки выше символ "(", то в приводимый тип
                                    # будет захватываться еще и ") as"
                                    new_type += ')'
                                types_of_fields[full_name][column_name_and_type['name']]['new_type'] = new_type

            # Проверка, что у полей внутри coalesce заданы одинаковые типы полей
            # в конструкции ::new_type
            # проверка на наличие coalesce в коде ноды сделана потому, что иначе будут стрелять варианты
            # когда поле формируется из нескольких полей разных типов, а они все прописываются в блоке CASE ... END
            if types_of_fields and 'coalesce' in node['params'].get('sql', ''):
                new_types = [j['new_type'] or j['old_type'] for i in types_of_fields.values() for j in i.values()]
                count_unique_types = len(set(new_types))
                assert count_unique_types == 1, f"В ноде {node['task_id']} не совпадают новые типы полей, " \
                                                f"формирующих колонку {column_name}:\n"\
                                                f"{new_types}"

            node['col_types'].update({column_name: types_of_fields})

        # Обработка случаев когда внутри конструкции coalesce нет приведения типов полей к новым значениям
        for column_name in node['col_types']:
            coalesce_pattern = f'coalesce\((.*)\)\s*(as)*\s*{column_name}'
            is_coalesce = re.findall(coalesce_pattern, node['params'].get('sql', ''), re.IGNORECASE)
            if is_coalesce:
                for coalesce_part in is_coalesce:
                    coalesce_types = set()
                    fields, as_ = coalesce_part
                    for field in fields.split(','):
                        _, field_name = field.strip().split('.') if '.' in field else ('', '')
                        if '::' in field_name:
                            field_name = field_name.split('::')[0].strip()
                        if ')' in field_name:
                            field_name = field_name.split(')')[0].strip()

                        try:
                            if field_name:
                                field_types = list(node['col_types'][column_name].values())[0][field_name]
                        except KeyError:
                            pre_pre_types = [i for i in list(node['col_types'][column_name].values()) if
                                             field_name in i]
                            pre_types = list(pre_pre_types[0].values()) if pre_pre_types else []
                            field_types = pre_types[0] if pre_types else dict()
                        if field_types:
                            field_type_for_add = field_types['new_type'] if field_types.get('new_type') else \
                            field_types['old_type']
                            coalesce_types.add((field_name, field_type_for_add))

                    all_types = ',\n'.join([f'{k}={v}' for k, v in coalesce_types])
                    count_unique_types = {i[1] for i in coalesce_types}
                    assert len(count_unique_types) == 1, f"В ноде {node['task_id']} есть coalesce, " \
                                                         f"в котором указаны поля разных типов " \
                                                         f"и отсутствует приведение к одному типу:\n{all_types}"


@pytest.mark.integration
@allure.id("3487830")
@allure.feature('Скрипты')
@allure.story('Хардкод')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'В SQL скрипте используются тестовые или wrk-схемы')
@allure.description('Проверяет отсутствие захардкоженной тестовой схемы или wrk-схемы в SQL скриптах')
def test_schema_in_sql_script(package, sql_scripts) -> None:
    sql_scripts = sql_scripts if isinstance(sql_scripts, list) else [sql_scripts]
    for script in sql_scripts:
        with allure.step(f'Открываем файл, берем текст скрипта {script.name}'):
            script_text = script.get_content()

        with allure.step('Ищем таблицы со схемами test_, vldw, _wrk (вместо etl_wrk) или work-схема джоба'):
            table_name_regexp = \
                rf'(?i)\btest_\w+\.\w+|\bvldw\w+\.\w+|\ddw\w+\.\w+|\btdw\w+\.\w+'
            wrong_table_names = re.findall(table_name_regexp, script_text)
            assert not wrong_table_names, f'В скрипте {script.name} найдены таблицы, имеющие некорректную схему: ' \
                                          f'{wrong_table_names}'


@pytest.mark.integration
@allure.id("5272123")
@allure.feature('Скрипты')
@allure.story('Хардкод')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'DLH: В SQL скрипте используются тестовые или wrk-схемы')
@allure.description('Проверяет отсутствие захардкоженной тестовой схемы или wrk-схемы в SQL скриптах')
def test_schema_in_sql_script_for_dlh(package, sql_scripts) -> None:
    sql_scripts = sql_scripts if isinstance(sql_scripts, list) else [sql_scripts]
    for script in sql_scripts:
        with allure.step(f'Открываем файл, берем текст скрипта {script.name}'):
            script_text = script.get_content()

        with allure.step('Ищем захардкоженные схемы'):
            table_name_regexp = \
                rf'(?i)\btest_\w+\.\w+|\bvldw\w+\.\w+|\ddw\w+\.\w+|\btdw\w+\.\w+'
            wrong_table_names = re.findall(table_name_regexp, script_text)
            assert not wrong_table_names, f'В скрипте {script.name} найдены таблицы, имеющие некорректную схему: ' \
                                          f'{wrong_table_names}'


@pytest.mark.integration
@allure.id("4458295")
@allure.feature('Скрипты')
@allure.story('Схемы')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'В SQL скриптах найдены схемы, отсутствующие на проде')
@allure.description("""
Тест проверяет, что в SQL скриптах из пакета не указаны схемы, которых нет на проде.

Что делать в случае падения теста:
- проверить есть ли схема на проде
- проверить наличие опечаток в коде
- проверить подтянул ли DataSync данную схему на ваш контур - если нет, то нужно дождаться окончания синхронизации
(поскольку список схем на проде мы берем именно из DataSync) и перезапустить тест
""")
def test_unknown_schema_in_sql_script(package,
                                      GP_integration,
                                      schemas_in_scripts,
                                      datasync_backup_dates,
                                      contour_short_name) -> None:
    script_name, tables = schemas_in_scripts
    tables = list(set(tables))
    with allure.step('Получаем список схем с прода, используя Data Sync'):
        first_day_prefix, second_day_prefix = datasync_backup_dates
        if contour_short_name == 'chimera_prodlike':
            # для prodlike меняем первую дату на стандартный префикс
            first_day_prefix = 'prod'
        datasync_query = """
            SELECT DISTINCT(t.table_schema)
            FROM (SELECT replace(table_schema, '{date1}', '') AS table_schema
                  FROM INFORMATION_SCHEMA.tables
                  WHERE table_schema LIKE '{date1}_%'
                  UNION ALL
                  SELECT replace(table_schema, '{date2}', '') AS table_schema
                  FROM information_schema.tables
                  WHERE table_schema LIKE '{date2}_%') AS t
        """.format(
            date1=first_day_prefix,
            date2=second_day_prefix
        )
        schemas = [i[0].lower() for i in GP_integration.executeAndReturnLists(datasync_query)]

        # поиск схем для TEA таблиц
        s3_tables = CoreConsumersAPIClient().get_tables(
            consumer_name='ETL.DLH',
            cluster_type='S3'
        )
        # берем схему каждой таблицы и отсекаем символы "prod" из  названия
        schemas += {i['name'].lower().split('.')[0][4:] for i in s3_tables}

        extra_schemas = ['_etl_wrk']
        schemas.extend(extra_schemas)

    with allure.step('Ищем ошибки в схемах'):
        wrong_tables = set(filter(lambda x: x.split('.')[0] not in schemas, tables))
        assert not wrong_tables, \
            f'В скрипте {script_name} найдены схемы, отсутствующие на проде: {wrong_tables}'


@pytest.mark.integration
@allure.id("5284470")
@allure.feature('Скрипты')
@allure.story('Схемы')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'В Spark скриптах найдены схемы, отсутствующие на проде')
@allure.description("""
Тест проверяет, что в Spark скриптах из пакета не указаны схемы, которых нет на проде.

Что делать в случае падения теста:
- проверить есть ли схема на проде
- проверить наличие опечаток в коде
""")
def test_unknown_schema_in_sql_script_for_dlh(package,
                                              dlh_integration,
                                              schemas_in_scripts) -> None:
    script_name, script_schemas = schemas_in_scripts
    script_schemas = list(set(['prod' + i if i.startswith('_') else i for i in script_schemas]))
    with allure.step('Получаем список схем из DLH'):
        get_schemas_in_dlh_query = """SHOW SCHEMAS;"""
        schemas = [i[0].lower() for i in dlh_integration.execute(get_schemas_in_dlh_query)]
        extra_schemas = ['_etl_wrk']
        schemas.extend(extra_schemas)

    with allure.step('Ищем ошибки в схемах'):
        wrong_tables = set(filter(lambda x: x not in schemas, script_schemas))
        assert not wrong_tables, \
            f'В скрипте {script_name} найдены схемы, отсутствующие на проде: {wrong_tables}'


@pytest.mark.integration
@allure.id("1959248")
@allure.feature('Cut')
@allure.story('Валидация настроек в JSON')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'В настройках cut2 не задано table_distribution')
@allure.description('Проверяем, что для всех подрезаемых таблиц указано поле распределения')
def test_cut2_distribution_key_in_dags(package_dag, cut2_service_api, contour_short_name) -> None:
    with allure.step("Get Cut2 settings."):
        if contour_short_name == 'chimera_test':
            cut2_settings = cut2_service_api['test_api'].get_cut_params(package_dag.name)
        else:
            cut2_settings = cut2_service_api['dev_api'].get_cut_params(package_dag.name)
    with allure.step('Check distributed key in jobs'):
        for tab in cut2_settings.get('tables'):
            meta_name = tab.get('meta_name', '')
            table_distribution = tab.get('table_distribution')
            cut_view_flg = tab.get('cut_view_flg', 0)

            # DPQA-1061: дистрибуция не требуется, если cut_view_flg=1
            if cut_view_flg != '1':
                assert table_distribution, f'key distribution for table {meta_name} is empty'
            else:
                assert not table_distribution, f'key distribution for {meta_name} must not be'


@pytest.mark.integration
@allure.id("5821785")
@allure.feature('Cut')
@allure.story('Валидация настроек в JSON')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Поле, заданное в настройках cut2 в table_distribution, присутствует в источнике')
@allure.description('Проверяем, что поле распределения времянки с катом есть в источнике DAGа')
def test_cut2_distribution_field_exists_in_source(package_dag,
                                                  cut2_service_api,
                                                  contour_short_name,
                                                  vial_prefix,
                                                  package) -> None:
    with allure.step("Получаем настройки CUT2"):
        if contour_short_name == 'chimera_test':
            cut2_settings = cut2_service_api['test_api'].get_cut_params(package_dag.name)
        else:
            cut2_settings = cut2_service_api['dev_api'].get_cut_params(package_dag.name)

    with allure.step('Получаем поля распределения таблиц из настроек CUT2'):
        distributed_keys = []

        for table in cut2_settings.get('tables'):
            table_name = f"{table['table_schema']}.{table['table_name']}"
            if vial_prefix not in table_name:
                schema, tbl = set_prefix_in_schema(vial_prefix, table_name)
            column_of_distribution = table.get('table_distribution')
            distributed_keys.append({
                'table_name': f'{schema}.{tbl}',
                'column_of_distribution': column_of_distribution
            })

    with allure.step('Проверяем наличие полей распределения в источниках'):
        for table in distributed_keys:
            columns_info = package.get_all_columns_with_types()[table['table_name']]
            columns_in_table = [col_name for col_name, col_type, max_length in columns_info]
            column_of_distribution = table['column_of_distribution']
            # добавил проверку на существование поля распределения из-за того
            # что table_distribution теперь необязательное поле, если вьюха строится
            if column_of_distribution and (column_of_distribution not in columns_in_table):
                assert f"Поле распределения {table['column_of_distribution']} из настроек CUT2 " \
                       f"не найдено в источнике {table['table_name']}!"


@pytest.mark.integration
@allure.id("2256794")
@allure.feature('Job (Dag)')
@allure.story('Обсолет')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'Попытка запуска дагов переводимых в obsolete')
@allure.description('Проверяет отсутствие дагов, переводимых в обсолет, в шаге Schedule')
def test_delete_dag_not_in_schedule(package_dag) -> None:
    """Checks for any deleted, renamed DAGs in the Schedule step."""

    with allure.step('Find all dags in schedule.'):
        assert not package_dag, f'deleted or renamed dag {package_dag.name} in Schedule step'


@pytest.mark.integration
@allure.id("2238843")
@allure.feature('Cut')
@allure.story('Валидация настроек в JSON')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Остались настройки кат после удаленного дага')
@allure.description('Проверяет, что удаляются настройки Cut2 при переименовании/удалении дагов')
def test_delete_cut2_sett_for_dag(package_dag, package, cut2_service_api, old_dag) -> None:
    """Checks that Cut2 settings are removed when renaming/deleting DAGs:"""
    with allure.step("Check Cut2 settings for altered/obsoleted dag."):
        deleted_gags = package.get_scenario_objects('TediDeleteDag')
        if package_dag.name != old_dag or package_dag.name in deleted_gags:
            cut2_settings = cut2_service_api['test_api'].get_cut_params(old_dag)
            assert not cut2_settings, f'not removed cut2 settings for {old_dag}'


@pytest.mark.integration
@allure.id('2387065')
@allure.feature('Job (Dag)')
@allure.story('Настройки лоадеров')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'Настройки SCD на проде и тесте отличаются')
@allure.description('Проверяет не менялись ли настройки лоадера')
def test_compare_loader_params_with_prod(package_dag_by_repo, production_dag) -> None:
    with allure.step("Get vial and prod loader params and compare."):
        params_vial = [params['params'] for params in package_dag_by_repo.transforms if
                       params.get('type').startswith('tedi.sinks')]
        params_prod = [params['params'] for params in production_dag.transforms if
                       params.get('type').startswith('tedi.sinks')]
        params_keys = ['compare', 'conn_id', 'key', 'load_method', 'business_keys', 'rebuild_history_mode',
                       'use_partitions']

        def check_params(params_vial, params_prod, keys):
            if params_vial.get('load_method') and params_prod.get('load_method'):
                for key in keys:
                    with allure.step(f"Check params for: {key}"):
                        prod_value = params_prod.get(key)
                        vial_value = params_vial.get(key)
                        assert vial_value == prod_value, f'Значения настройки {key} отличаются: ' \
                                                         f'на Проде = {prod_value}, текущие = {vial_value}'
            else:
                assert params_vial, "Change LOADER type, is correct?"

        if len(params_vial) > 1:
            for vial, prod in zip(params_vial, params_prod):
                check_params(vial, prod, params_keys)
        else:
            check_params(params_vial[0], params_prod[0], params_keys)


@pytest.mark.integration
@allure.id('5303948')
@allure.feature('Job (Dag)')
@allure.story('Настройки лоадеров')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: Настройки SCD на проде и тесте отличаются')
@allure.description('Проверяет не менялись ли настройки лоадера')
def test_compare_loader_params_with_prod_for_dlh(package_dag_by_repo, production_dag) -> None:
    with allure.step("Get vial and prod loader params and compare."):
        params_vial = [params['params'] for params in package_dag_by_repo.transforms if
                       params.get('type').startswith('tedi.sinks.spark_scd1')]
        params_prod = [params['params'] for params in production_dag.transforms if
                       params.get('type').startswith('tedi.sinks.spark_scd1')]
        params_keys = ['compare', 'conn_id', 'key', 'load_method', 'business_keys', 'rebuild_history_mode',
                       'use_partitions']

        def check_params(params_vial, params_prod, keys):
            if params_vial.get('load_method') and params_prod.get('load_method'):
                for key in keys:
                    with allure.step(f"Check params for: {key}"):
                        prod_value = params_prod.get(key)
                        vial_value = params_vial.get(key)
                        assert vial_value == prod_value, f'Значения настройки {key} отличаются: ' \
                                                         f'на Проде = {prod_value}, текущие = {vial_value}'
            else:
                assert params_vial, "Change LOADER type, is correct?"

        if len(params_vial) > 1:
            for vial, prod in zip(params_vial, params_prod):
                check_params(vial, prod, params_keys)
        else:
            check_params(params_vial[0], params_prod[0], params_keys)


@pytest.mark.integration
@allure.id('2851469')
@allure.feature('Job (Dag)')
@allure.story('Настройки ID_TRANSLATE')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Настройки idtr на проде и тесте отличаются')
@allure.description('Проверяет различие в количестве и параметрах нод IDTR на проде и тесте.')
def test_compare_id_translate_params_with_prod(package_dag_by_repo, production_dag, package) -> None:
    with allure.step('Получаем список IDTranslate с теста и прода.'):
        no_obsolete_dags = {dag.Name for dag in package.getMetaObjects('Dag{!o}')}
        if package_dag_by_repo.name in no_obsolete_dags:
            test_idtr_params = package_dag_by_repo.get_dags_idtr_params(convert_to_str=True)
            prod_idtr_params = production_dag.get_dags_idtr_params(convert_to_str=True)
        else:
            test_idtr_params = []
            prod_idtr_params = []

    with allure.step('Проверяем изменения в нодах IDTranslate.'):
        error_string = ''

        diff_cnt = len(test_idtr_params) - len(prod_idtr_params)
        if diff_cnt > 0:
            error_string += '\nНа тесте используется больше IDTranslate\'ов\n'
        elif diff_cnt < 0:
            error_string += '\nНа проде используется больше IDTranslate\'ов\n'

        idtrs_only_on_test = ''.join(sorted([idtr for idtr in test_idtr_params if idtr not in prod_idtr_params]))
        idtrs_only_on_prod = ''.join(sorted([idtr for idtr in prod_idtr_params if idtr not in test_idtr_params]))

        error_string_diff = ''.join(ndiff(idtrs_only_on_prod.splitlines(keepends=True),
                                          idtrs_only_on_test.splitlines(keepends=True)))

        error_string = f'{error_string}\nDiff изменений:\n{error_string_diff}' \
            if error_string_diff else ''
        assert not bool(error_string), error_string


@pytest.mark.integration
@allure.id('2877060')
@allure.feature('Job (Dag)')
@allure.story('Настройки ID_TRANSLATE')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Нет фильтра на NULL в ноде после ID_TRANSLATE с Generate RK=False;')
@allure.description('Тест проверяет, что если Primary Key таргета формируется из IDTR, '
                    'то в этой IDTR параметр Generate RK=True; '
                    'либо есть фильтрация наллов по этому RK в следующей ноде.')
def test_generate_rk_is_true_for_pk(package_dag) -> None:
    with allure.step('Получаем ключи дага из лоадера.'):
        loader = package_dag.transforms[-1]
        if loader['type'].endswith('SCD1'):
            keys = loader['params']['key'] if 'params' in loader.keys() else loader['key']
        elif loader['type'].endswith('SCD2'):
            keys = loader['params']['business_keys'] if 'params' in loader.keys() else loader['business_keys']
        else:
            pytest.skip('Лоадер не SCD1/SCD2 или не найдены ключи в лоадере.')

        # Attachment
        with allure.step(f'Ключи таргета: {keys}'):
            pass

    with allure.step('Проверяем, что если ключ получается из IDTR, то в IDTR идет генерация этого rk.'):
        error_string = '\nPK {0} формируется из IDTR, но не генерируется в ней.\n' \
                       'Также нет фильтрации наллов по этому rk в следующей ноде.\n' \
                       'Могут возникнуть наллы по ключу. Возможно, не ошибка.'

        nodes_with_generated_rk = list()
        for i in range(len(package_dag.transforms)):
            node = package_dag.transforms[i]
            if node['type'].endswith('IDTranslateChain'):
                if node['params']['field_rk'] in keys and not any([id_tr.get('generate_rk', False)
                                                                   for id_tr in node['params']['bk_params']]):
                    nodes_with_generated_rk.append((i, node['params']['field_rk']))

            elif node['type'].endswith('IDTranslate'):
                for id_tr in node['params']['bk_params']:
                    if id_tr['field_rk'] in keys and not id_tr.get('generate_rk', False):
                        nodes_with_generated_rk.append((i, id_tr['field_rk']))

        for source_node_id, field in nodes_with_generated_rk:
            error = True
            # Attachment
            with allure.step(f'Нет генерации {field} в IDTR (нода {package_dag.transforms[source_node_id]["task_id"]}).'
                             f'\nПроверяем, что в следующей ноде есть фильтрация наллов по этому ключу.'):
                pass

            for j in range(source_node_id + 1, len(package_dag.transforms)):
                tgt_node = package_dag.transforms[j]
                if package_dag.transforms[source_node_id]['task_id'] in tgt_node.get('source', []):
                    # Attachment
                    with allure.step(f"Проверяем ноду {tgt_node['task_id']}."):
                        pass

                    code = tgt_node['params'].get('sql', '') if 'params' in tgt_node.keys() else tgt_node.get('sql', '')
                    if re.search(rf'\b(\w+\.)?{field.lower()}\s+is\s+not\s+null', code.lower()):
                        # пренебрегаем случаем, когда из IDTR выходят две и более ноды, в одной из которых есть фильтр,
                        # в других нет
                        error = False
                        break

            assert not error, error_string.format(field)
