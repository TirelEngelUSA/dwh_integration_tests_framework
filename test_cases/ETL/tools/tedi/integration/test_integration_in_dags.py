import re
import psycopg2
import pytest
import allure
from collections import defaultdict

from Framework.pytest_custom.CustomExceptions import NoScenarion
from Framework.utils.ddl_compare import CompareDdl
from Framework.utils.spec_testing_template_util import ConfigObject
from Config.chimera_scenario_steps import ScenarioSteps
from Framework.ETLobjects.dag import TediGpTransforms



@pytest.mark.integration
@allure.id("2028907")
@allure.feature('Tables')
@allure.story('ETL зависимости. Зависимые даги')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От переименованной таблицы или удаленных/переименованных столбцов есть зависимые даги')
@allure.description('Проверяет, есть ли в зависимых дагах колонки, которые были дропнуты или переименованы')
def test_depend_dags_with_drop_or_rename_columns(depend_dag, tedi_gp_target, change_column) -> None:
    """
    Проверяет зависимости дагов от изменившихся таблиц в сочетания таблиц - колонка:
        1) Получаем все даги из пакета и код трансформа зависимого дага. Получаем код каждого трансформа в разрезе его имени.
        2) Проверяем трансформ: номер = {номер трансформа}, имя = {имя трансформа}. Проверяет каждый трансформ отдельно на отсутствие использования таблицы + колонки, которые пришли в тест (включая алиасы).
    """
    with allure.step('Получаем код ноды зависимого дага'):
        transforms = depend_dag.get_transforms_code()

    reg_exp = r'([^a-z,^_]|,)'
    for num, transform in enumerate(transforms):
        with allure.step('Проверяем трансформ: номер = {0}, имя = {1}'.format(str(num + 1), transform['task_id'])):
            code = transform.get('sql')
            if code is None:
                if transform.get('params'):
                    code = transform['params'].get('sql')
            if 'GreenplumSQL' in transform['type']:
                assert code, "Не смог получить SQL код ноды"
            else:
                if code is None:
                    continue
            alias = depend_dag.get_table_alias_from_code(tedi_gp_target, code)
            table_key_name = '_'.join(tedi_gp_target.replace('.', '_').split('_')[1:]).upper()
            code_part = "{{ ti\.xcom_pull\(key='" + table_key_name + "'\) }}"
            alias_by_key_name = depend_dag.get_table_alias_from_code(code_part, code)
            alias = alias + alias_by_key_name
            for name in alias:
                find_use_str = "{0}.{1}".format(name, change_column)
                assert re.search(reg_exp + find_use_str + reg_exp,
                                 code.lower()) is None, f'Алиас таблицы и измененная колонка ({find_use_str}) ' \
                                                        f'используется в трансформе {transform["task_id"]} ' \
                                                        f'зависимого дага {depend_dag.name}.'


@pytest.mark.integration
@allure.id("5329881")
@allure.feature('Tables')
@allure.story('ETL зависимости. Зависимые даги')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: От переименованной таблицы или удаленных/переименованных столбцов есть зависимые даги')
@allure.description('Проверяет, есть ли в зависимых дагах колонки, которые были дропнуты или переименованы')
def test_depend_dags_with_drop_or_rename_columns_for_dlh(depend_dag, tedi_dlh_target, change_column) -> None:
    """
    Проверяет зависимости дагов от изменившихся таблиц в сочетания таблиц - колонка:
        1) Получаем все даги из пакета и код трансформа зависимого дага. Получаем код каждого трансформа в разрезе его имени.
        2) Проверяем трансформ: номер = {номер трансформа}, имя = {имя трансформа}. Проверяет каждый трансформ отдельно на отсутствие использования таблицы + колонки, которые пришли в тест (включая алиасы).

    """
    with allure.step('Получаем код ноды зависимого дага'):
        transforms = depend_dag.get_transforms_code()

    reg_exp = r'([^a-z,^_]|,)'
    for num, transform in enumerate(transforms):
        with allure.step('Проверяем трансформ: номер = {0}, имя = {1}'.format(str(num + 1), transform['task_id'])):
            code = transform.get('sql')
            if code is None:
                if transform.get('params'):
                    code = transform['params'].get('sql')
            if 'GreenplumSQL' in transform['type']:
                assert code, "Не смог получить SQL код ноды"
            else:
                if code is None:
                    continue
            alias = depend_dag.get_table_alias_from_code(tedi_dlh_target, code)
            table_key_name = '_'.join(tedi_dlh_target.replace('.', '_').split('_')[1:]).upper()
            code_part = "{{ ti\.xcom_pull\(key='" + table_key_name + "'\) }}"
            alias_by_key_name = depend_dag.get_table_alias_from_code(code_part, code)
            alias = alias + alias_by_key_name
            for name in alias:
                find_use_str = "{0}.{1}".format(name, change_column)
                assert re.search(reg_exp + find_use_str + reg_exp,
                                 code.lower()) is None, f'Алиас таблицы и измененная колонка ({find_use_str}) ' \
                                                        f'используется в трансформе {transform["task_id"]} ' \
                                                        f'зависимого дага {depend_dag.name}.'


@pytest.mark.integration
@allure.id("4984593")
@allure.feature('Tables')
@allure.story('ETL зависимости. Настройки ката зависимых объектов')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Удаленные/переименованные столбцы используются в настройках CUT у зависимого дага')
@allure.description('Проверяет, есть ли в настройках CUT у зависимых дагов колонки,'
                    ' которые были дропнуты или переименованы')
def test_depend_cut_setting_after_drop_or_rename_columns(depend_dag, tedi_gp_target,
                                                         change_column, cut2_service_api) -> None:
    """
    Проверяет не используется ли удаляемая колонка в настройках CUT у зависимого дага:
        1) Получаем все даги из пакета и настройки CUT зависимого дага.
        2) Проверяет что в настройках CUT не используется удаляемое поле.

    """
    with allure.step('Получаем настройки CUT зависимого дага'):
        cut2_settings = cut2_service_api['prod_api'].get_cut_params(depend_dag.name)

    with allure.step('Проверяем наличие поля в настройках CUT'):
        used_columns = []
        if cut2_settings:
            for table in cut2_settings['tables']:
                pref = tedi_gp_target.split('_')[0]
                target_table = tedi_gp_target.replace(pref + '_', '<>_', 1).upper()
                table_in_cut_setting = f"{table['table_schema']}.{table['table_name']}".upper()
                if target_table == table_in_cut_setting and table.get('cut_filter') \
                        and change_column in table['cut_filter']:
                    used_columns.append(change_column)

        assert not used_columns, f'Измененная колонка ({change_column}) ' \
                                 f'используется в настройках CUT {depend_dag.name}.'


@pytest.mark.integration
@allure.id("5279081")
@allure.feature('Tables')
@allure.story('ETL зависимости. Настройки ката зависимых объектов')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: Удаленные/переименованные столбцы используются в настройках CUT у зависимого дага')
@allure.description('Проверяет, есть ли в настройках CUT у зависимых дагов колонки,'
                    ' которые были дропнуты или переименованы')
def test_depend_cut_setting_after_drop_or_rename_columns_for_dlh(depend_dag, tedi_dlh_target,
                                                                 change_column, cut2_service_api) -> None:
    """
    Проверяет не используется ли удаляемая колонка в настройках CUT у зависимого дага:
        1) Получаем все даги из пакета и настройки CUT зависимого дага.
        2) Проверяет что в настройках CUT не используется удаляемое поле.

    """
    with allure.step('Получаем настройки CUT зависимого дага'):
        cut2_settings = cut2_service_api['prod_api'].get_cut_params(depend_dag.name)

    with allure.step('Проверяем наличие поля в настройках CUT'):
        used_columns = []
        if cut2_settings:
            for table in cut2_settings['tables']:
                pref = tedi_dlh_target.split('_')[0]
                target_table = tedi_dlh_target.replace(pref + '_', '<>_', 1).upper()
                table_in_cut_setting = f"{table['table_schema']}.{table['table_name']}".upper()
                if target_table == table_in_cut_setting and table.get('cut_filter') \
                        and change_column in table['cut_filter']:
                    used_columns.append(change_column)

        assert not used_columns, f'Измененная колонка ({change_column}) ' \
                                 f'используется в настройках CUT {depend_dag.name}.'


@pytest.mark.integration
@allure.id("3216091")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'В репозитории вьюх обнаружена уникальная вьюха с явным указанием поля, '
                       'которое меняется или удаляется в таблице-источнике')
@allure.description("""
Проверяет есть ли в репозитории https://gitlab.tcsbank.ru/dwh-core-public/gpviews/-/tree/master/data/views_unique уникальная вьюха, у которой:
- в коде явно перечисляются поля из источников
- в текущем пакете меняется набор полей или их имена хотя бы в одном из ее источников
- несовпадение типов в полях, переданных в coalesce в нодах DAGа.

Что делать если тест упал:
- Открыть репозиторий gpviews
- Найти уникальные вьюхи, перечисленные в тесте
- Проверить что в скрипте создания вьюхи есть поля, которые были изменены в источнике, но не изменены во вьюхе
- Поправить эти поля во вьюхе согласно инструкции <a href="https://wiki.tcsbank.ru/pages/viewpage.action?pageId=3590212254">Если нужно пересоздать уникальную вью, на которую не настроен байнд джоб </a>

Что делать если тест сломался
- Посмотреть на проблему с которой упал тест:
    - если она описана в Известных проблемах - то подписываемся на задачу с правкой и ждем ее исправления
    - если она не указана нигде - заводим обращение в боте
""")
def test_depend_unique_view_from_gptable_with_changed_column(gp_table, gitlab_views_parser,
                                                             mg_replica_helper, package,
                                                             vial_prefix) -> None:
    with allure.step('Парсинг репозитория вьюх и формирование набора измененных полей'):
        assert gp_table.startswith('<>_'), f'{gp_table} ожидается в формате: <>_schema_name.table_name'
        correct_phys_name = gp_table.replace('<>', '<deployment_mode>', 1)
        exception_text = ''
        depend_views = None
        dags_to_update = {dag.Name for dag in package.getMetaObjects('Dag{s}')}
        table_to_obsolete = {table.Name for table in package.getMetaObjects('GpTable{o}')}
        etl_process_loads = mg_replica_helper.get_etl_process_loads_target(gp_table.replace('<>_', 'prod_'))
        dag_name = ''
        for etl_process in etl_process_loads:
            if etl_process[1] == 'DAG':
                dag_name = etl_process[0]
                break
        # Если даг BIND, то поля ДО разработки получаем из репозитория view (GpViews)
        if '_bind_' in dag_name:
            view_path = gp_table.replace('<>_', 'prod_').replace('.', '/')
            all_stash_views_link = gitlab_views_parser.all_stash_views_link()
            for stash_views_link in all_stash_views_link:
                if f'{view_path}.sql' in stash_views_link[0]:
                    old_columns = gitlab_views_parser.get_columns_of_view(stash_views_link)
                    break
            assert old_columns, f'Из репозитория не получены поля вьюхи {gp_table} до доработки'
            # Если таблица обсолетится, то за изменяемые поля принимаем все поля из бэкапа
            if gp_table in table_to_obsolete:
                change_columns = old_columns
            else:
                change_columns = package.getChangeColumsOfTable(gp_table, table_type='GpTable',
                                                                contour_prefix=vial_prefix, old_columns=old_columns)[1]
        else:
            _, change, old = package.getChangeColumsOfTable(gp_table, table_type='GpTable',
                                                            contour_prefix=vial_prefix)
            assert old, f'Не получены поля таблицы {gp_table} до доработки'
            # Если таблица обсолетится, то за изменяемые поля принимаем все поля из бэкапа
            if gp_table in table_to_obsolete:
                change_columns = old
            else:
                change_columns = change
        if change_columns:
            depend_views = gitlab_views_parser.get_table_dependent_views(correct_phys_name)

    with allure.step('Поиск переименованных/удаленных колонок в коде зависимых вью, если в пакете нет bind дага'):
        if depend_views and change_columns:
            used_columns_in_depend_views = dict()
            for view_name, view_code in depend_views.items():
                # Из репозитория имя возвращается = имени файла, избавляемся от его расширения
                view_name = view_name.replace('.sql', '')
                *_, schema, name = view_name.split('/')
                view_name = schema + '.' + name
                bind_dag_of_view = [entity[0] for entity in mg_replica_helper.get_etl_process_loads_target(view_name)]

                if not bind_dag_of_view:
                    possible_bind_name = ConfigObject.phys_name_to_dag_name(view_name, load_or_bind='bind')
                    bind_dag_of_view.append(possible_bind_name)

                if bind_dag_of_view and bind_dag_of_view[0] in dags_to_update:
                    continue

                for change_column in change_columns:
                    if change_column not in view_code:
                        continue

                    used_columns_in_depend_views.setdefault(view_name, []).append(change_column)

            if used_columns_in_depend_views:
                exception_text = 'Колонки таблицы переименованы либо удалены. Проверьте зависимые вью: '
                for view_name, columns in used_columns_in_depend_views.items():
                    url = gitlab_views_parser.gitlab_views_url + view_name
                    allure.dynamic.link(url, name="Ссылка на вью: {0}".format(view_name))
                    exception_text += f'\n{view_name} использует колонки {columns};'

        assert exception_text == '', exception_text


@pytest.mark.integration
@allure.id("5277386")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: В репозитории вьюх обнаружена уникальная вьюха с явным указанием поля, '
                       'которое меняется или удаляется в таблице-источнике')
@allure.description("""
Проверяет есть ли в репозитории https://gitlab.tcsbank.ru/dwh-core-public/gpviews/-/tree/master/data/views_unique уникальная вьюха, у которой:
- в коде явно перечисляются поля из источников
- в текущем пакете меняется набор полей или их имена хотя бы в одном из ее источников
- несовпадение типов в полях, переданных в coalesce в нодах DAGа.

Что делать если тест упал:
- Открыть репозиторий gpviews
- Найти уникальные вьюхи, перечисленные в тесте
- Проверить что в скрипте создания вьюхи есть поля, которые были изменены в источнике, но не изменены во вьюхе
- Поправить эти поля во вьюхе согласно инструкции <a href="https://wiki.tcsbank.ru/pages/viewpage.action?pageId=3590212254">Если нужно пересоздать уникальную вью, на которую не настроен байнд джоб </a>

Что делать если тест сломался
- Посмотреть на проблему с которой упал тест:
    - если она описана в Известных проблемах - то подписываемся на задачу с правкой и ждем ее исправления
    - если она не указана нигде - заводим обращение в боте
""")
def test_depend_unique_view_from_dlhtable_with_changed_column(dlh_table, gitlab_views_parser,
                                                              mg_replica_helper, package,
                                                              vial_prefix) -> None:
    with allure.step('Парсинг репозитория вьюх и формирование набора измененных полей'):
        assert dlh_table.startswith('<>_'), f'{dlh_table} ожидается в формате: <>_schema_name.table_name'
        correct_phys_name = dlh_table.replace('<>', '<deployment_mode>', 1)
        exception_text = ''
        depend_views = None
        dags_to_update = {dag.Name for dag in package.getMetaObjects('Dag{s}')}
        table_to_obsolete = {table.Name for table in package.getMetaObjects('DlhTable{o}')}
        etl_process_loads = mg_replica_helper.get_etl_process_loads_target(dlh_table.replace('<>_', 'prod_'))
        dag_name = ''
        for etl_process in etl_process_loads:
            if etl_process[1] == 'DAG':
                dag_name = etl_process[0]
                break
        # Если даг BIND, то поля ДО разработки получаем из репозитория view (GpViews)
        if '_bind_' in dag_name:
            view_path = dlh_table.replace('<>_', 'prod_').replace('.', '/')
            all_stash_views_link = gitlab_views_parser.all_stash_views_link()
            for stash_views_link in all_stash_views_link:
                if f'{view_path}.sql' in stash_views_link[0]:
                    old_columns = gitlab_views_parser.get_columns_of_view(stash_views_link)
                    break
            assert old_columns, f'Из репозитория не получены поля вьюхи {dlh_table} до доработки'
            # Если таблица обсолетится, то за изменяемые поля принимаем все поля из бэкапа
            if dlh_table in table_to_obsolete:
                change_columns = old_columns
            else:
                change_columns = package.getChangeColumsOfTable(dlh_table, table_type='DlhTable',
                                                                contour_prefix=vial_prefix, old_columns=old_columns)[1]
        else:
            _, change, old = package.getChangeColumsOfTable(dlh_table, table_type='DlhTable',
                                                            contour_prefix=vial_prefix)
            assert old, f'Не получены поля таблицы {dlh_table} до доработки'
            # Если таблица обсолетится, то за изменяемые поля принимаем все поля из бэкапа
            if dlh_table in table_to_obsolete:
                change_columns = old
            else:
                change_columns = change
        if change_columns:
            depend_views = gitlab_views_parser.get_table_dependent_views(correct_phys_name)

    with allure.step('Поиск переименованных/удаленных колонок в коде зависимых вью, если в пакете нет bind дага'):
        if depend_views and change_columns:
            used_columns_in_depend_views = dict()
            for view_name, view_code in depend_views.items():
                # Из репозитория имя возвращается = имени файла, избавляемся от его расширения
                view_name = view_name.replace('.sql', '')
                *_, schema, name = view_name.split('/')
                view_name = schema + '.' + name
                bind_dag_of_view = [entity[0] for entity in mg_replica_helper.get_etl_process_loads_target(view_name)]

                if not bind_dag_of_view:
                    possible_bind_name = ConfigObject.phys_name_to_dag_name(view_name, load_or_bind='bind')
                    bind_dag_of_view.append(possible_bind_name)

                if bind_dag_of_view and bind_dag_of_view[0] in dags_to_update:
                    continue

                for change_column in change_columns:
                    if change_column not in view_code:
                        continue

                    used_columns_in_depend_views.setdefault(view_name, []).append(change_column)

            if used_columns_in_depend_views:
                exception_text = 'Колонки таблицы переименованы либо удалены. Проверьте зависимые вью: '
                for view_name, columns in used_columns_in_depend_views.items():
                    url = gitlab_views_parser.gitlab_views_url + view_name
                    allure.dynamic.link(url, name="Ссылка на вью: {0}".format(view_name))
                    exception_text += f'\n{view_name} использует колонки {columns};'

        assert exception_text == '', exception_text


@pytest.mark.integration
@allure.id("4941808")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомных вьюх от таблицы с новой колонкой')
@allure.description('Проверяет существование кастомной вьюхи, зависящей от таблицы, '
                    'в которой были добавлены поля. Не обязательно ошибка, просто напоминание!')
def test_depend_custom_view_from_gptable_with_new_column(tables_with_new_columns, gitlab_views_parser,
                                                         mg_replica_helper, package) -> None:
    with allure.step('Проверка наличия зависимых кастомных вью, если есть новые поля'):
        correct_phys_name = tables_with_new_columns.replace('<>', '<deployment_mode>', 1)
        depend_views = gitlab_views_parser.get_table_dependent_views(correct_phys_name)

    with allure.step('Проверка наличия/отсутствия в пакете bind дага, соответствующему кастомной вьюхи'):
        exception_text = ''
        dags_to_update = {job.Name for job in package.getMetaObjects('Dag{s}')}
        if depend_views:
            used_table_in_depend_views = dict()
            for view_name in depend_views:
                # Из репозитория имя возвращается = имени файла, избавляемся от его расширения
                view_name = view_name.replace('.sql', '')
                *_, schema, name = view_name.split('/')
                view_name = schema + '.' + name
                bind_dag_of_view = [entity[0] for entity in mg_replica_helper.get_etl_process_loads_target(view_name)]

                if not bind_dag_of_view:
                    possible_bind_name = ConfigObject.phys_name_to_dag_name(view_name, load_or_bind='bind')
                    bind_dag_of_view.append(possible_bind_name)

                if bind_dag_of_view and bind_dag_of_view[0] in dags_to_update:
                    continue

                used_table_in_depend_views.setdefault(view_name, []).append(tables_with_new_columns)

            if used_table_in_depend_views:
                exception_text = 'По таблице, в которой добавлены поля, имеются зависимые вью: '
                for view_name, table in used_table_in_depend_views.items():
                    url = gitlab_views_parser.gitlab_views_url + view_name
                    allure.dynamic.link(url, name="Ссылка на вью: {0}".format(view_name))
                    exception_text += f'\n{view_name} использует таблицу {table};'

        assert exception_text == '', exception_text


@pytest.mark.integration
@allure.id("4969639")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомных вьюх от вьюх с новой колонкой')
@allure.description('Проверяет существование кастомной вьюхи, зависящей от таблицы, '
                    'в которой были добавлены поля. Не обязательно ошибка, просто напоминание!')
def test_depend_custom_view_from_gpview_with_new_column(tables_with_new_columns, gitlab_views_parser,
                                                        mg_replica_helper, package) -> None:
    with allure.step('Проверка наличия зависимых кастомных вью, если есть новые поля'):
        correct_phys_name = tables_with_new_columns.replace('<>', '<deployment_mode>', 1)
        depend_views = gitlab_views_parser.get_table_dependent_views(correct_phys_name)

    with allure.step('Проверка наличия/отсутствия в пакете bind дага, соответствующему кастомной вьюхи'):
        exception_text = ''
        dags_to_update = {job.Name for job in package.getMetaObjects('Dag{s}')}
        if depend_views:
            used_table_in_depend_views = dict()
            for view_name in depend_views:
                # Из репозитория имя возвращается = имени файла, избавляемся от его расширения
                view_name = view_name.replace('.sql', '')
                *_, schema, name = view_name.split('/')
                view_name = schema + '.' + name
                bind_dag_of_view = [entity[0] for entity in mg_replica_helper.get_etl_process_loads_target(view_name)]

                if not bind_dag_of_view:
                    possible_bind_name = ConfigObject.phys_name_to_dag_name(view_name, load_or_bind='bind')
                    bind_dag_of_view.append(possible_bind_name)

                if bind_dag_of_view and bind_dag_of_view[0] in dags_to_update:
                    continue

                used_table_in_depend_views.setdefault(view_name, []).append(tables_with_new_columns)

            if used_table_in_depend_views:
                exception_text = 'По таблице, в которой добавлены поля, имеются зависимые вью: '
                for view_name, table in used_table_in_depend_views.items():
                    url = gitlab_views_parser.gitlab_views_url + view_name
                    allure.dynamic.link(url, name="Ссылка на вью: {0}".format(view_name))
                    exception_text += f'\n{view_name} использует таблицу {table};'

        assert exception_text == '', exception_text


@pytest.mark.integration
@allure.id("4594207")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомной вьюхи после переименования BIND -> LOAD')
@allure.description('Находит даги, которые переименовываются в пакете BIND -> LOAD'
                    'Проверяет удаление кастомной вьюхи в этом же пакете')
def test_depend_custom_view_from_gptable_after_rename_bind(tedi_dag, package_dag, package, chimera_api, vial_prefix) -> None:
    with allure.step('Получение таргета дага'):
        targets = package_dag.get_target_tables()
        assert targets, f"Таргеты дага {package_dag} не обнаружены"

        # Не усложняем - считаем, что в рамках переименования bind дага не ожидается таргетов > 1
        dag_target = targets[0]
        # От теста так же ожидается префикс test_, поэтому некий костыль
        prefixes = [vial_prefix, 'test']
        for prefix in prefixes:
            if dag_target.startswith(prefix):
                dag_target = dag_target.replace(prefix, '<>', 1)
        assert dag_target, f'Таргет дага {tedi_dag} не получен'
    with allure.step('Проверка наличия шага drop_unique_view_meta в сценарии'):
        check_flg = False
        package_scenario = chimera_api.get_package_scenario(package.name)
        for scenario_step in package_scenario:
            if scenario_step['step_type'] == 'drop_unique_view_meta':
                for parameter in scenario_step['parameters']:
                    if parameter['name'] == 'view_names':
                        for view in parameter['value']:
                            if view == dag_target:
                                check_flg = True
        assert check_flg, f'В сценарии не найден шаг удаления кастомной вью: {dag_target}'


@pytest.mark.integration
@allure.id("2434646")
@allure.feature('Tables')
@allure.story('ETL зависимости')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'От переименованной таблицы и удаленных/переименованных столбцов есть зависимые даги')
@allure.description('Проверяет, есть ли в зависимых дагах новое имя таблицы и нет ли удаленных/переименованных колонок.')
def test_dags_depend_on_renamed_tables(tedi_gp_target, target_old_name, rename_flag, depend_dag, deploy_parser_obj,
                                       vial_prefix, GP_integration, table_factory) -> None:
    """
    Проверяет наличие зависимостей:
        1) от старого имени таблицы (если она переименовывалась);
        2) от удаленных/переименованных колонок, используемых в зависимых.
    """
    with allure.step('Получаем список источников зависимого дага.'):
        depend_dag_sources = depend_dag.get_source_tables()
        depend_dag_sources.sort()
        with allure.step('\n'.join(depend_dag_sources)):
            pass

    with allure.step(f'Проверяем, что новое имя таблицы есть в списке источников (нет зависимости на старое имя)'):
        formatted_gp_target = tedi_gp_target.replace('test_', '').replace('.', '_').upper()
        assert formatted_gp_target in depend_dag_sources, 'Измененная таблица {0} используется ' \
                                                          'в зависимом даге {1} ' \
                                                          'как источник.'.format(formatted_gp_target,
                                                                                 depend_dag_sources)

    with allure.step('Проверяем, что все колонки, которые используются в зависимом даге, '
                     'есть и в самой таблице'):
        transforms = depend_dag.get_transforms_code()
        target_table = table_factory.generate_table_by_test_name(tedi_gp_target, vial_prefix)
        error_strings = set()
        try:
            target_columns = target_table.get_columns_from_phys()
        except Exception as e:
            error = str(e).split('\n')[0]
            error_strings.add(error)

        assert len(error_strings) == 0, "Ошибки при обращении к БД:\n{0}". \
            format('\n'.join([f'{i + 1}) {err}' for i, err in enumerate(error_strings)]))

        for num, transform in enumerate(transforms):
            with allure.step('Проверяем ноду #{0}. {1}'.format(str(num + 1), transform['task_id'])):
                code = transform['params'].get('sql')
                if code is None:
                    continue
                code = code.lower()
                alias = depend_dag.get_table_alias_from_code(tedi_gp_target, code)
                table_key_name = '_'.join(tedi_gp_target.replace('.', '_').split('_')[1:]).upper()
                code_part = "{{ ti\.xcom_pull\(key='" + table_key_name + "'\) }}"
                alias_by_code = depend_dag.get_table_alias_from_code(code_part, code)
                alias = alias + alias_by_code
                check_columns = deploy_parser_obj.find_columns_in_code(alias, code)

                if check_columns:
                    with allure.step('Таблица используется в ноде'):
                        with allure.step(f'Код ноды:\n\n {code}'):
                            pass
                        with allure.step(f'Алиасы таблицы: \n\n {alias}'):
                            pass

                    wrong_columns = set()
                    for column in check_columns:
                        if column not in target_columns:
                            wrong_columns.add(column)

                    if len(wrong_columns) != 0:
                        step_attachment = 'Некоторые колонки, используемые в ноде, не найдены в таблице: \n' \
                                          + str(wrong_columns)
                        with allure.step(step_attachment):
                            pass

                    assert len(wrong_columns) == 0, "Колонки {0} используются в даге {1}, но отсутствуют в физике {2}".\
                        format(', '.join(wrong_columns), depend_dag.name, target_table.name)


@pytest.mark.integration
@allure.id("5344223")
@allure.feature('Tables')
@allure.story('ETL зависимости')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: От переименованной таблицы или удаленных/переименованных столбцов есть зависимые даги')
@allure.description('Проверяет, есть ли в зависимых дагах новое имя таблицы и нет ли удаленных/переименованных колонок.')
def test_dags_depend_on_renamed_tables_for_dlh(tedi_dlh_target, target_old_name, rename_flag, depend_dag, deploy_parser_obj,
                                       vial_prefix, GP_integration, table_factory) -> None:
    """
    Проверяет наличие зависимостей:
        1) от старого имени таблицы (если она переименовывалась);
        2) от удаленных/переименованных колонок, используемых в зависимых.
    """
    with allure.step('Получаем список источников зависимого дага.'):
        depend_dag_sources = depend_dag.get_source_tables()
        depend_dag_sources.sort()
        with allure.step('\n'.join(depend_dag_sources)):
            pass

    with allure.step(f'Проверяем, что новое имя таблицы есть в списке источников (нет зависимости на старое имя)'):
        formatted_gp_target = tedi_dlh_target.replace('test_', '').replace('.', '_').upper()
        assert formatted_gp_target in depend_dag_sources, 'Измененная таблица {0} используется ' \
                                                          'в зависимом даге {1} ' \
                                                          'как источник.'.format(formatted_gp_target,
                                                                                 depend_dag_sources)

    with allure.step('Проверяем, что все колонки, которые используются в зависимом даге, '
                     'есть и в самой таблице'):
        transforms = depend_dag.get_transforms_code()
        target_table = table_factory.generate_table_by_test_name(tedi_dlh_target, vial_prefix)
        error_strings = set()
        try:
            target_columns = target_table.get_columns_from_phys()
        except Exception as e:
            error = str(e).split('\n')[0]
            error_strings.add(error)

        assert len(error_strings) == 0, "Ошибки при обращении к БД:\n{0}". \
            format('\n'.join([f'{i + 1}) {err}' for i, err in enumerate(error_strings)]))

        for num, transform in enumerate(transforms):
            with allure.step('Проверяем ноду #{0}. {1}'.format(str(num + 1), transform['task_id'])):
                code = transform['params'].get('sql')
                if code is None:
                    continue
                code = code.lower()
                alias = depend_dag.get_table_alias_from_code(tedi_dlh_target, code)
                table_key_name = '_'.join(tedi_dlh_target.replace('.', '_').split('_')[1:]).upper()
                code_part = "{{ ti\.xcom_pull\(key='" + table_key_name + "'\) }}"
                alias_by_code = depend_dag.get_table_alias_from_code(code_part, code)
                alias = alias + alias_by_code
                check_columns = deploy_parser_obj.find_columns_in_code(alias, code)

                if check_columns:
                    with allure.step('Таблица используется в ноде'):
                        with allure.step(f'Код ноды:\n\n {code}'):
                            pass
                        with allure.step(f'Алиасы таблицы: \n\n {alias}'):
                            pass

                    wrong_columns = set()
                    for column in check_columns:
                        if column not in target_columns:
                            wrong_columns.add(column)

                    if len(wrong_columns) != 0:
                        step_attachment = 'Некоторые колонки, используемые в ноде, не найдены в таблице: \n' \
                                          + str(wrong_columns)
                        with allure.step(step_attachment):
                            pass

                    assert len(wrong_columns) == 0, "Колонки {0} используются в даге {1}, но отсутствуют в физике {2}".\
                        format(', '.join(wrong_columns), depend_dag.name, target_table.name)


@pytest.mark.integration
@allure.id("4052994")
@allure.feature('Tables')
@allure.story('Prototype')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Источники таблицы соответствуют источникам прототипа на Wiki')
@allure.description('Проверяет соответствие источников таблицы в прототипе на Wiki и в DAGе')
def test_prototype_sources_coincide_with_wiki_for_dags(package_dag, tedi_gp_target, wiki_client, vial_prefix, package) -> None:
    """Сравниваем источники, которые есть в коде построения прототипа с итоговым списком источников в DAGе"""
    with allure.step('Get prototype code from Wiki'):
        pref = tedi_gp_target.split('_')[0]
        tedi_gp_target = tedi_gp_target.replace(pref + '_', '<>_')
        urls = wiki_client.get_potential_dags_wiki_urls(tedi_gp_target)
        allure.attach(urls[0], name='Wiki page 1', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[0]), name=urls[0])
        allure.attach(urls[1], name='Wiki page 2', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[1]), name=urls[1])

        prot = wiki_client.get_prototype(urls)
        if prot:
            allure.attach(prot, name='Prototype code', attachment_type=allure.attachment_type.TEXT)

        assert prot, "Can't find prototype code on Wiki"

    with allure.step("Clear from comments"):
        result = []
        stack = []  # Стек для отслеживания комментариев

        i = 0
        prot_len = len(prot)
        count_characters_in_comments = 2  # '/*' '*/' '--'

        while i < prot_len:
            if i < prot_len - 1 and prot[i:i + count_characters_in_comments] == '/*':  # Начало многострочного комментария
                stack.append(i)  # Сохраняем индекс начала комментария
                i += count_characters_in_comments
            elif i < prot_len - 1 and prot[i:i + count_characters_in_comments] == '*/' and stack:  # Конец многостр. комм.
                stack.pop()  # Удаляем последний открытый комментарий
                i += count_characters_in_comments
            elif i < prot_len - 1 and prot[i:i + count_characters_in_comments] == '--':  # Начало однострочного комментария
                while i < prot_len and prot[i] != '\n':  # Пропускаем до конца строки
                    i += 1
            else:
                if not stack:  # Если нет активных комментариев, добавляем символ
                    result.append(prot[i])
                i += 1

        prot = ''.join(result)

    with allure.step('Get DAG sources'):
        dag_tables = package_dag.get_source_tables()
        with allure.step('Add DAG targets'):
            dag_targets = package_dag.get_target_tables()
            for dag_target in dag_targets:
                dag_tables.append(dag_target[dag_target.find('_') + 1:].replace('.', '_').upper())

    with allure.step('Get prototype sources'):
        reg = r'(?i)(from|join)\s+((<>_|\w+_)\w+\.\w+)'
        tables_matchs = re.findall(reg, prot)
        tables_from_prot = []
        for match in tables_matchs:
            table = match[1].lower()
            if not table.split('.')[0].endswith('wrk'):
                if table.startswith('prod') or table.startswith('test') or table.startswith('<>'):
                    index = table.find('_') + 1
                else:
                    index = 0
                tables_from_prot.append(table[index:].replace('.', '_').upper())

    with allure.step('Check source from prototype exist in DAG sources'):
        not_in_sources = []
        for table in tables_from_prot:
            if table not in dag_tables:
                not_in_sources.append(table)

        assert not not_in_sources, f'Tables from prototype not in sources of DAG - {package_dag.name}'


@pytest.mark.integration
@allure.id("2484491")
@allure.feature('Tables')
@allure.story('DDL')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'ddl таблицы в GP соответствует описанию на wiki')
@allure.description('Проверяет соответствие названий и состава полей в wiki и ddl')
def test_ddl_coincide_with_wiki_for_dags(gp_table, wiki_client, vial_prefix, package) -> None:
    """Compare table DDL with table structure on Wiki:
        Get columns from physics in GP - get columns from physics
        Check correct naming in physical columns - check matching of column names to pattern (1st - 'a-z', then 'a-z0-9_') in physics
        Get columns from Wiki - get wiki urls from table name and get column names from wiki.
        Check correct naming on Wiki - check matching of column names to pattern (1st - 'a-z', then 'a-z0-9_') on Wiki
        Check extra columns in physics - Get a list of columns from physics that do not match with Wiki
        Check extra columns on Wiki - Get a list of columns from Wiki that do not match with physics
        """

    column_naming_rule = r"\A[a-zA-Z][a-zA-Z0-9_]*\Z"

    with allure.step('Get columns from physics in GP'):
        c = CompareDdl()
        phys_table = vial_prefix+gp_table[2:]
        phys_columns = c.get_set_of_columns_for_given_table(phys_table)
        assert len(phys_columns) != 0, f"\nTable {phys_table} does not exist.\n"
        allure.attach(str(list(phys_columns)), name='Physical columns', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Check correct naming in physical columns'):
        phys_bad_columns = [i for i in phys_columns if not re.match(column_naming_rule, i)]
        assert len(phys_bad_columns) == 0, (
            "\n"
            "There are not allowed characters in physical column names.\n"
            "Allowed characters: 1st - 'a-z', then 'a-z0-9_'\n"
            f"Table: {gp_table}\n"
            f"Columns with not allowed characters: {phys_bad_columns}\n"
        )

    with allure.step('Get columns from Wiki'):
        urls = wiki_client.get_potential_dags_wiki_urls(gp_table)
        allure.attach(urls[0], name='Wiki page 1', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[0]), name=urls[0])
        allure.attach(urls[1], name='Wiki page 2', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[1]), name=urls[1])
        wiki_columns = wiki_client.get_wiki_columns(urls)
        allure.attach(str(list(wiki_columns)), name='Columns from Wiki', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Check correct naming on Wiki'):
        wiki_bad_columns = [i for i in wiki_columns if not re.match(column_naming_rule, i)]
        assert len(wiki_bad_columns) == 0, (
            "\n"
            "There are not allowed characters in Wiki column names.\n"
            "Allowed characters: 1st - 'a-z', then 'a-z0-9_'\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
            f"Columns with not allowed characters: {wiki_bad_columns}\n"
        )

    with allure.step(f'Check extra columns in physics'):
        jira_final_statuses = frozenset(['Done', 'ACKNOWLEDGEMENT'])
        diff_phys_wo_wiki = phys_columns - wiki_columns
        if diff_phys_wo_wiki:
            columns_info = wiki_client.get_column_and_task_info(urls, diff_phys_wo_wiki)

            for column, info in columns_info.items():
                if info['is_strike'] and info['task'] != package.name \
                        and info['task_status'] not in jira_final_statuses:
                    diff_phys_wo_wiki.remove(column)
        assert len(diff_phys_wo_wiki) == 0, (
            "\n"
            "There are physical columns not mentioned on Wiki.\n"
            f"Table: {gp_table}\n"
            f"Columns in physics that don't match to Wiki: {diff_phys_wo_wiki}.\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
        )

    with allure.step(f'Check extra columns on Wiki'):
        diff_wiki_wo_phys = wiki_columns - phys_columns
        if diff_wiki_wo_phys:
            columns_info = wiki_client.get_column_and_task_info(urls, diff_wiki_wo_phys)
            for column, info in columns_info.items():
                if info['task'] != package.name and info['task_status'] not in jira_final_statuses:
                    diff_wiki_wo_phys.remove(column)

        assert len(diff_wiki_wo_phys) == 0, (
            "\n"
            "There are columns on Wiki that are absent in physics.\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
            f"Columns on Wiki that don't match to physics: {diff_wiki_wo_phys}.\n"
            f"Table: {gp_table}\n"
        )


@pytest.mark.integration
@allure.id("5273314")
@allure.feature('Tables')
@allure.story('DDL')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: ddl таблицы в DLH соответствует описанию на wiki')
@allure.description('Проверяет соответствие названий и состава полей в wiki и ddl')
def test_ddl_coincide_with_wiki_for_dags_for_dlh(dlh_phys_table, wiki_client, vial_prefix, package) -> None:
    """Compare table DDL with table structure on Wiki:
        Get columns from physics in DLH - get columns from physics
        Check correct naming in physical columns - check matching of column names to pattern (1st - 'a-z', then 'a-z0-9_') in physics
        Get columns from Wiki - get wiki urls from table name and get column names from wiki.
        Check correct naming on Wiki - check matching of column names to pattern (1st - 'a-z', then 'a-z0-9_') on Wiki
        Check extra columns in physics - Get a list of columns from physics that do not match with Wiki
        Check extra columns on Wiki - Get a list of columns from Wiki that do not match with physics
        """

    column_naming_rule = r"\A[a-zA-Z][a-zA-Z0-9_]*\Z"

    with allure.step('Get columns from physics in DLH'):
        phys_columns = dlh_phys_table.get_columns_from_phys()
        assert len(phys_columns) != 0, f"\nTable {dlh_phys_table.phys_name} does not exist.\n"
        allure.attach(str(list(phys_columns)), name='Physical columns', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Check correct naming in physical columns'):
        phys_bad_columns = [i for i in phys_columns if not re.match(column_naming_rule, i)]
        assert len(phys_bad_columns) == 0, (
            "\n"
            "There are not allowed characters in physical column names.\n"
            "Allowed characters: 1st - 'a-z', then 'a-z0-9_'\n"
            f"Table: {dlh_phys_table.phys_name}\n"
            f"Columns with not allowed characters: {phys_bad_columns}\n"
        )

    with allure.step('Get columns from Wiki'):
        dlh_table = dlh_phys_table.phys_name.replace(dlh_phys_table.prefix, '<>')
        urls = wiki_client.get_potential_dags_wiki_urls(dlh_table)
        allure.attach(urls[0], name='Wiki page 1', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[0]), name=urls[0])
        allure.attach(urls[1], name='Wiki page 2', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[1]), name=urls[1])
        wiki_columns = wiki_client.get_wiki_columns(urls)
        allure.attach(str(list(wiki_columns)), name='Columns from Wiki', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Check correct naming on Wiki'):
        wiki_bad_columns = [i for i in wiki_columns if not re.match(column_naming_rule, i)]
        assert len(wiki_bad_columns) == 0, (
            "\n"
            "There are not allowed characters in Wiki column names.\n"
            "Allowed characters: 1st - 'a-z', then 'a-z0-9_'\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
            f"Columns with not allowed characters: {wiki_bad_columns}\n"
        )

    with allure.step(f'Check extra columns in physics'):
        jira_final_statuses = frozenset(['Done', 'ACKNOWLEDGEMENT'])
        diff_phys_wo_wiki = phys_columns - wiki_columns
        if diff_phys_wo_wiki:
            columns_info = wiki_client.get_column_and_task_info(urls, diff_phys_wo_wiki)

            for column, info in columns_info.items():
                if info['is_strike'] and info['task'] != package.name \
                        and info['task_status'] not in jira_final_statuses:
                    diff_phys_wo_wiki.remove(column)
        assert len(diff_phys_wo_wiki) == 0, (
            "\n"
            "There are physical columns not mentioned on Wiki.\n"
            f"Table: {dlh_table}\n"
            f"Columns in physics that don't match to Wiki: {diff_phys_wo_wiki}.\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
        )

    with allure.step(f'Check extra columns on Wiki'):
        diff_wiki_wo_phys = wiki_columns - phys_columns
        if diff_wiki_wo_phys:
            columns_info = wiki_client.get_column_and_task_info(urls, diff_wiki_wo_phys)
            for column, info in columns_info.items():
                if info['task'] != package.name and info['task_status'] not in jira_final_statuses:
                    diff_wiki_wo_phys.remove(column)

        assert len(diff_wiki_wo_phys) == 0, (
            "\n"
            "There are columns on Wiki that are absent in physics.\n"
            f"Links to Wiki:\n{urls[0]}\n{urls[1]}\n"
            f"Columns on Wiki that don't match to physics: {diff_wiki_wo_phys}.\n"
            f"Table: {dlh_table}\n"
        )


@allure.id('2501446')
@pytest.mark.integration
@allure.feature('Tables')
@allure.story('Обсолет')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'Корректность перевода таблицы в обсолет')
@allure.description('Проверяем что не осталось дагов, зависящих от переводимой в обсолет таблицы')
def test_obsol_dag_tables(package_dag_by_repo, package, vial_prefix, mg_replica_helper, sas_script_parser) -> None:
    with allure.step('Получаем список дагов(s) из конфига'):
        current_dags = {dag.Name for dag in package.getMetaObjects('Dag{}')}
        renamed_objects = {package._get_obj_name_before_renaming(dag, 'Dag') for dag in current_dags}
        config_dags = current_dags | renamed_objects

        dags_to_print = '\n'.join(config_dags)
        with allure.step(dags_to_print):
            pass

    with allure.step('Получаем таргет дага'):
        target_tables = package_dag_by_repo.get_target_tables()

        if not target_tables:
            for transform in package_dag_by_repo.transforms:
                if transform['type'] == TediGpTransforms.gp_view.value:
                    target_tables.extend(list(transform['params']['entity_target'].values()))

        obsolete_table = ''

        if target_tables:
            obsolete_table = target_tables[0].replace('test_', 'prod_').replace('<>_', 'prod_')
            obsolete_table = obsolete_table.lower()

        assert target_tables, 'DAG не содержит таргетов в Greenplum! Проверьте вручную'

        with allure.step(obsolete_table):
            pass

    with allure.step('Получаем urn таблицы в DataDetective'):
        result = mg_replica_helper.get_urn_of_object(obsolete_table)
        assert result, 'Таблица не найдена в DataDetective. Необходимо проверить зависимости вручную.'

        urn, phys_table = result[0]
        url = f'https://dd.tcsbank.ru/{urn}'
        allure.dynamic.link(url, name=f'Link to DataDetective: {phys_table}')

        with allure.step(urn):
            pass

    with allure.step('Ищем в DataDetective зависимые от таблицы даги'):
        dep_dags = {obj[0] for obj in mg_replica_helper.get_depend_etl_objects_by_urn(urn) if obj[1] == 'DAG'}

        dags_to_print = '\n'.join(sorted(dep_dags))
        with allure.step(dags_to_print):
            pass

    if dep_dags:
        with allure.step('Проверяем, что зависимые даги есть в конфиге'):
            depend_not_in_config = dep_dags - config_dags

            assert not depend_not_in_config, f'От таблицы {obsolete_table} зависят объекты, ' \
                                             f'которых нет в конфиге: {depend_not_in_config}. ' \
                                             f'Необходимо проверить, что все зависимости учтены.'

    with allure.step('Проверяем, что таблица переводится в обсолет'):
        obsolete_tables_from_package = {table.Name.replace('<>_', 'prod_') for table
                                        in package.getMetaObjects('GpTable{o}')}
        assert obsolete_table in obsolete_tables_from_package, f'В пакете не найдена таблица {obsolete_table} ' \
                                                               f'с флагом Obsolete. ' \
                                                               f'Проверьте перевод таблицы в обсолет вручную.'


@allure.id('5311538')
@pytest.mark.integration
@allure.feature('Tables')
@allure.story('Обсолет')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'Корректность перевода DLH таблицы в обсолет')
@allure.description('Проверяем что не осталось дагов, зависящих от переводимой в обсолет таблицы')
def test_obsol_dag_tables_for_dlh(package_dag_by_repo, package, vial_prefix, mg_replica_helper, sas_script_parser) -> None:
    with allure.step('Получаем список дагов(s) из конфига'):
        current_dags = {dag.Name for dag in package.getMetaObjects('Dag{}')}
        renamed_objects = {package._get_obj_name_before_renaming(dag, 'Dag') for dag in current_dags}
        config_dags = current_dags | renamed_objects

        dags_to_print = '\n'.join(config_dags)
        with allure.step(dags_to_print):
            pass

    with allure.step('Получаем таргет дага'):
        target_tables = package_dag_by_repo.get_dlh_targets()
        obsolete_table = ''

        if target_tables:
            obsolete_table = target_tables[0].replace('test_', 'prod_')
            obsolete_table = obsolete_table.lower()

        assert target_tables, 'DAG не содержит таргетов в DLH! Проверьте вручную'

        with allure.step(obsolete_table):
            pass

    with allure.step('Получаем urn таблицы в DataDetective'):
        result = mg_replica_helper.get_urn_of_object(obsolete_table)
        assert result, 'Таблица не найдена в DataDetective. Необходимо проверить зависимости вручную.'

        urn, phys_table = result[0]
        url = f'https://dd.tcsbank.ru/{urn}'
        allure.dynamic.link(url, name=f'Link to DataDetective: {phys_table}')

        with allure.step(urn):
            pass

    with allure.step('Ищем в DataDetective зависимые от таблицы даги'):
        dep_dags = {obj[0] for obj in mg_replica_helper.get_depend_etl_objects_by_urn(urn) if obj[1] == 'DAG'}

        dags_to_print = '\n'.join(sorted(dep_dags))
        with allure.step(dags_to_print):
            pass

    if dep_dags:
        with allure.step('Проверяем, что зависимые даги есть в конфиге'):
            depend_not_in_config = dep_dags - config_dags

            assert not depend_not_in_config, f'От таблицы {obsolete_table} зависят объекты, ' \
                                             f'которых нет в конфиге: {depend_not_in_config}. ' \
                                             f'Необходимо проверить, что все зависимости учтены.'

    with allure.step('Проверяем, что таблица переводится в обсолет'):
        obsolete_tables_from_package = {table.Name.replace('<>_', 'prod_') for table
                                        in package.getMetaObjects('DlhTable{o}')}
        assert obsolete_table in obsolete_tables_from_package, f'В пакете не найдена таблица {obsolete_table} ' \
                                                               f'с флагом Obsolete. ' \
                                                               f'Проверьте перевод таблицы в обсолет вручную.'


@pytest.mark.integration
@allure.id("2424677")
@allure.feature('Job (Dag)')
@allure.story('Настройки ката в даге')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Не совпадают фильтры в настройках cut2 и даге')
@allure.description('Проверяет что фильтры из настроек CUT2 есть в фильтрах первых нод дага')
def test_check_cut_filters_in_dag(package_dag, cut2_service_api, vial_prefix):
    with allure.step("Получаем sql каждой ноды"):
        nodes = package_dag.get_transforms_code()
        cut_filters_in_settings = set()
        cut_filters_in_dag = set()
        symbols_to_exclude = [' ', '(', ')', ':', '-', '_', '/', '\n']

        # Attachment
        step_attachment = f'1. С сервера теди получаем sql-код нод дага.\n' \
                          f'Для виала: https://k8s-vial-master-1.ds.prod-tedi.local/tedi/api/v1/dags/{package_dag.name}\n\n' \
                          f'2. Задаем символы для исключения из кода нод и фильтра ката (symbols_to_exclude):\n' \
                          f'{symbols_to_exclude}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Получаем настройки кат'):
        if vial_prefix.startswith('t'):
            table_settings = [table for table in cut2_service_api['test_api'].get_cut_params(package_dag.name)['tables']
                              if table.get('cut_filter')]
        else:
            table_settings = [table for table in cut2_service_api['dev_api'].get_cut_params(package_dag.name)['tables']
                              if table.get('cut_filter')]

        # Attachment
        step_attachment = f'1. Получаем настройки ката из сервиса Cut2.\n' \
                          f'Для виала: http://preview-etl-cut-dwh.tcsbank.ru/api/v3/jobs/{package_dag.name}\n\n' \
                          f'2. Извлекаем фильтры из структуры "tables" (таблица: фильтры):\n'

        for table in table_settings:
            cut_filter = table.get('cut_filter')
            split_pattern = r'(?i)\b(?:or|and)\b'
            split_filters = [cut_filter.strip() for cut_filter in re.split(split_pattern, cut_filter)]
            cut_filters_in_settings.update(split_filters)

            # Attachment
            step_attachment += f'{table.get("meta_name")}:\n{cut_filter}\n\n'

        # Attachment
        cut_filters_in_settings_attach = '\n'.join(cut_filters_in_settings)
        step_attachment += f'\n3. Разбиваем фильтры на части, разделенные AND и OR. ' \
                           f'Записываем в cut_filters_in_settings:\n' \
                           f'{cut_filters_in_settings_attach}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем фильтры ката'):
        filters_before_removing = dict()
        cut_filters_in_settings_copy = cut_filters_in_settings.copy()
        for filter_before_removing in cut_filters_in_settings_copy:
            filter_after_removing = filter_before_removing.lower()

            """
            Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
            а в кат effective_to_dttm = '5999-01-01'
            """
            date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
            filter_after_removing = re.sub(date_pattern, r"'\1 00:00:00'", filter_after_removing)

            filter_after_removing = ''.join([symbol for symbol in filter_after_removing
                                             if symbol not in symbols_to_exclude])

            """
            В даге может быть current_date, current_timestamp, а в кат now()::date, now()
            """
            filter_after_removing = filter_after_removing.replace('nowdate', 'currentdate')
            filter_after_removing = filter_after_removing.replace('nowtimestamp', 'currenttimestamp')
            filter_after_removing = filter_after_removing.replace('currentdatedate', 'currentdate')
            filter_after_removing = filter_after_removing.replace('now', 'currenttimestamp')
            filter_after_removing = filter_after_removing.replace('!=', '<>')

            cut_filters_in_settings.discard(filter_before_removing)
            cut_filters_in_settings.add(filter_after_removing)
            filters_before_removing[filter_after_removing] = filter_before_removing

        # Attachment
        step_attachment = f'В списке фильтров (cut_filters_in_settings) удаляем из каждого ' \
                          f'фильтра элементы symbols_to_exclude. Заменяем:\n' \
                          f'now()::date на current_date;\n' \
                          f'now()::timestamp на current_timestamp;\n' \
                          f'now() на current_timestamp.\n\n' \
                          f'Фильтры после преобразований:\n'
        step_attachment += '\n'.join(cut_filters_in_settings)
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем код каждой ноды'):
        # Attachment
        step_attachment = f'В коде ноды заменяем макросы:\n' \
                          f'"\'@@ execution_dt @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::timestamp" на "current_timestamp".\n' \
                          f'Удаляем символы из массива symbols_to_exclude.\n'

        for node in nodes:
            code = node['params'].get('sql', '').lower()

            """
            Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
            а в кат effective_to_dttm = '5999-01-01'
            """
            date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
            code = re.sub(date_pattern, r"'\1 00:00:00'", code)

            """
            Убираем префикс с названием таблицы: couchbase_application.dt_updated -> dt_updated
            """
            replace_pattern = r'(\(|,|>|<|=)\s*\w+\.'
            code = re.sub(replace_pattern, r'\1', code)

            code = ''.join([symbol for symbol in code
                            if symbol not in symbols_to_exclude])
            code = code.replace("'@@executiondt@@'date", 'currentdate')
            code = code.replace("'@@executionts@@'date", 'currentdate')
            code = code.replace("'@@executionts@@'timestamp", 'currenttimestamp')
            code = code.replace("'@@executionts@@'", 'currenttimestamp')
            code = code.replace("'@@executiondt@@'", 'currentdate')
            code = code.replace("cast'@@executiondt@@'asdate", 'currentdate')
            code = code.replace("cast'@@executiondt@@'astimestamp", 'currenttimestamp')
            code = code.replace('!=', '<>')

            node['params']['sql_clear'] = code
            node['cut_filter'] = list()

        with allure.step(step_attachment):
            pass

    with allure.step('Ищем фильтр ката в коде'):
        # Attachment
        step_attachment = f'1. В коде каждой ноды пытаемся найти фильтр из cut_filters_in_settings. Если нашли, ' \
                          f'добавляем в множество cut_filters_in_dag.\n'

        for node in nodes:
            code = node['params'].get('sql_clear', '')
            for cut_filter in cut_filters_in_settings:
                cut_filters_in_dag.add(cut_filter)
                node['cut_filter'].append(cut_filter)

        # Attachment
        filters_not_in_dag = [filters_before_removing[cut_filter] for cut_filter
                              in cut_filters_in_settings - cut_filters_in_dag]

        """
        Если нашли фильтры, которых нет в даге, то проверяем, что в них есть фильтры c in или not in.
        Например: seqno in (0,4,5).
        Т.к. в коде дага может быть три фильтра: seqno=0; seqno=4; seqno=5. Нужно проверить каждый отдельно.
        """
        filters_with_in = list()
        if filters_not_in_dag:
            in_pattern = r'(?i)(\s+|\))(not\s+in|in)\s*\(.*\)'
            filters_with_in = [{'filter_in_settings': cut_filter} for cut_filter in filters_not_in_dag
                               if re.findall(in_pattern, cut_filter)]

        if filters_with_in:
            with allure.step('Разбиваем фильтры ката с in на отдельные фильтры'):
                value_pattern = r"\'.*?\'|\d+"
                field_x_values_pattern = r'(?i)(.+?)\s*(?:not\s+in|in)\s*\((.+)\)'

                """
                Удаляем фильтры с in из filters_not_in_dag.
                Из фильтра получаем поле и значения в секции in.
                """
                for cut_filter in filters_with_in:
                    # удаляем фильтры с in из filters_not_in_dag
                    filters_not_in_dag.pop(filters_not_in_dag.index(cut_filter['filter_in_settings']))

                    in_flg = 'not in' not in cut_filter
                    field, in_values = re.findall(field_x_values_pattern, cut_filter['filter_in_settings'])[0]
                    in_values = re.findall(value_pattern, in_values)
                    in_values = {value: {'found_flg': False, 'filter': ''} for value in in_values}

                    cut_filter['field'] = field
                    cut_filter['in_values'] = in_values
                    cut_filter['in_flg'] = in_flg

                for cut_filter in filters_with_in:
                    """
                    Экранируем скобки, т.к. могут быть такие поля:
                    coalesce(mobile_phone_no,'#') not in ('88888888888', '8888888888')
                    """
                    field = re.sub(r'(\(|\))', r'\\\1', cut_filter['field'])
                    in_or_not_in = 'in' if cut_filter['in_flg'] else r'not\s+in'

                    """
                    Формируем паттерн для поиска фильтров с in в коде дага.
                    """
                    in_node_pattern = fr'(?is){field}\s*{in_or_not_in}\s*\(.*?\)'
                    cut_filter['in_node_pattern'] = in_node_pattern

                    """
                    Создаем фильтры для каждого значения, пример:
                    из balance_group_cd in('NIR','NIP') получаем balance_group_cd='NIR', balance_group_cd='NIP'
                    """
                    operator = '=' if cut_filter['in_flg'] else '<>'
                    for value, value_params in cut_filter['in_values'].items():
                        custom_filter = (field + operator + value).lower()
                        custom_filter = ''.join([symbol for symbol in custom_filter
                                        if symbol not in symbols_to_exclude])
                        value_params['filter'] = custom_filter

                        filters_before_removing[custom_filter] = cut_filter['filter_in_settings']

                # Attachment
                split_filters_attachment = ''
                for cut_filter in filters_with_in:
                    split_filters_attachment += cut_filter['filter_in_settings'] + ':\n\t'
                    split_filters_attachment += '\n\t'.join(
                        value_params['filter'] for value, value_params in cut_filter['in_values'].items())
                    split_filters_attachment += '\n\n'
                allure.attach(split_filters_attachment, name='Разделенные фильтры с in',
                              attachment_type=allure.attachment_type.TEXT)

                """
                Ищем фильтры в даге.
                """
                for node in nodes:
                    code = node['params'].get('sql_clear', '')
                    for cut_filter in filters_with_in:
                        for value_params in cut_filter['in_values'].values():
                            if value_params['filter'] in code:
                                value_params['found_flg'] = True
                                node['cut_filter'].append(value_params['filter'])

                for cut_filter in filters_with_in:
                    """
                    Если не для всех значений нашли фильтры, то ищем в даге фильтры с in.
                    Например в кате один фильтр balance_group_cd in ('NIP', 'NIR', 'NGP'), а в даге два:
                    balance_group_cd in ('NIP', 'NIR') и balance_group_cd in ('NGP').
                    """
                    if not all([value_params['found_flg'] for value, value_params in cut_filter['in_values'].items()]):
                        cut_filter['filters_in_dag'] = set()

                        for node in nodes:
                            code = node['params'].get('sql', '')
                            filters_in_dag = re.findall(cut_filter['in_node_pattern'], code)

                            """
                            Удаляем из фильтров дага символы из symbols_to_exclude, так же как и для ката.
                            """
                            for dag_filter in filters_in_dag:
                                dag_filter_after_removing = ''.join([symbol for symbol in dag_filter
                                                                     if symbol not in symbols_to_exclude])
                                cut_filter['filters_in_dag'].add(dag_filter_after_removing)

                                node['cut_filter'].append(dag_filter_after_removing)

                        """
                        Если нашли в даге фильтры с in, то проверяем есть ли эти значения в фильтрах ката с in.
                        """
                        if cut_filter['filters_in_dag']:
                            cut_filter['values_in_dag'] = set()
                            field = ''.join([symbol for symbol in cut_filter['field']
                                             if symbol not in symbols_to_exclude])
                            field = field + 'in' if cut_filter['in_flg'] else field + 'notin'

                            for dag_filter in cut_filter['filters_in_dag']:
                                values = re.sub(r'(?i)' + field, '', dag_filter)
                                values = values.split(',')
                                cut_filter['values_in_dag'].update(values)

                            for value, value_params in cut_filter['in_values'].items():
                                if value in cut_filter['values_in_dag']:
                                    value_params['found_flg'] = True

                in_filters_not_in_dag = [cut_filter['filter_in_settings'] for cut_filter in filters_with_in if
                                         not all(value_params['found_flg'] for value_params
                                                 in cut_filter['in_values'].values())]
                filters_not_in_dag.extend(in_filters_not_in_dag)

        # Attachment
        for num, node in enumerate(nodes):
            filters = '\n'.join(node.get('cut_filter', list()))
            step_attachment += f'\n#{num + 1}. "{node["task_id"]}". Фильтры:\n{filters}'

        filters_not_in_dag_attach = '\n'.join(filters_not_in_dag)
        step_attachment += f'\n\n2. Ищем фильтры в cut_filters_in_settings, которых нет в ' \
                           f'cut_filters_in_dag.\n\n' \
                           f'Ненайденные фильтры:\n' \
                           f'{filters_not_in_dag_attach}'
        with allure.step(step_attachment):
            pass

        assert filters_not_in_dag == [], 'Найдены фильтры в настройках ката, которых нет в коде дага.'


@pytest.mark.integration
@allure.id("5616434")
@allure.feature('Job (Dag)')
@allure.story('Настройки ката в даге')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Не совпадают фильтры в настройках дага и cut2')
@allure.description('Проверяет что фильтры из первых нод дага есть в фильтрах настроек CUT2')
def test_check_dag_filters_in_cut(package_dag, cut2_service_api, vial_prefix, ):
    with allure.step("Получаем sql каждой ноды"):
        from helpers.common import set_prefix_in_schema # используется чтобы унифицировать все таблицы к виду <>_
        from helpers.parsers import SqlParser

        nodes = package_dag.get_transforms_code()
        cut_filters_in_settings = defaultdict(list)
        raw_cut_filters_in_settings = defaultdict(list) #  Сырые фильтри в настройках - as is в настройках CUT
        cut_filters_in_dag = defaultdict(list)
        symbols_to_exclude = [' ', '(', ')', ':', '-', '_', '/', '\n']
        split_pattern = r'(?i)\b(?:or|and)\b'  # Паттерн сплита фильтра на отдельные логические элементы

        # Attachment
        step_attachment = f'1. С сервера теди получаем sql-код нод дага.\n' \
                          f'2. Задаем символы для исключения из кода нод и фильтра ката:\n' \
                          f'{symbols_to_exclude}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Получаем настройки кат'):
        if vial_prefix.startswith('t'):
            table_settings = [table for table in
                              cut2_service_api['test_api'].get_cut_params(package_dag.name)['tables']]
        else:
            table_settings = [table for table in
                              cut2_service_api['dev_api'].get_cut_params(package_dag.name)['tables']]

        # Attachment
        step_attachment = f'1. Получаем настройки ката из сервиса Cut2.\n' \
                          f'Для виала: http://preview-etl-cut-dwh.tcsbank.ru/api/v3/jobs/{package_dag.name}\n\n' \
                          f'2. Извлекаем фильтры из структуры "tables" (таблица: фильтры):\n'
        tables_from_cut = []
        for table in table_settings:
            cut_filter = table.get('cut_filter', None)
            if cut_filter:
                split_filters = [cut_filter.strip() for cut_filter in re.split(split_pattern, cut_filter)]
                table_schema, table_name = set_prefix_in_schema('<>', f"{table['table_schema']}.{table['table_name']}")
                raw_cut_filters_in_settings[f'{table_schema}.{table_name}'].extend(split_filters)

            # Attachment
            step_attachment += f'{table.get("meta_name")}:\n{cut_filter}\n\n'
            tables_from_cut.append(f"{table['table_schema']}.{table['table_name']}".lower())

        # Attachment
        cut_filters_in_settings_attach = '\n'.join(cut_filters_in_settings)
        step_attachment += f'\n3. Разбиваем фильтры на части, разделенные AND и OR. ' \
                           f'Записываем в cut_filters_in_settings:\n' \
                           f'{cut_filters_in_settings_attach}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем фильтры ката'):
        for table, filter_before_removing in raw_cut_filters_in_settings.items():
            for filter in filter_before_removing:
                filter_after_removing = filter.lower()

                """
                Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
                а в кат effective_to_dttm = '5999-01-01'
                """
                date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
                filter_after_removing = re.sub(date_pattern, r"'\1 00:00:00'", filter_after_removing)

                filter_after_removing = ''.join([symbol for symbol in filter_after_removing
                                                 if symbol not in symbols_to_exclude])

                """
                В даге может быть current_date, current_timestamp, а в кат now()::date, now()
                """
                filter_after_removing = filter_after_removing.replace('nowdate', 'currentdate')
                filter_after_removing = filter_after_removing.replace('nowtimestamp', 'currenttimestamp')
                filter_after_removing = filter_after_removing.replace('currentdatedate', 'currentdate')
                filter_after_removing = filter_after_removing.replace('now', 'currenttimestamp')
                filter_after_removing = filter_after_removing.replace('!=', '<>')

                cut_filters_in_settings[table].append(filter_after_removing)


        # Attachment
        step_attachment = f'В списке фильтров (cut_filters_in_settings) удаляем из каждого ' \
                          f'фильтра элементы symbols_to_exclude. Заменяем:\n' \
                          f'now()::date на current_date;\n' \
                          f'now()::timestamp на current_timestamp;\n' \
                          f'now() на current_timestamp.\n\n' \
                          f'Фильтры после преобразований:\n'
        step_attachment += '\n'.join(cut_filters_in_settings)
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем код каждой ноды'):
        # Attachment
        step_attachment = f'В коде ноды заменяем макросы:\n' \
                          f'"\'@@ execution_dt @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::timestamp" на "current_timestamp".\n' \
                          f'Удаляем символы из массива symbols_to_exclude.\n'
        for node in nodes:
            whole_filter_with_table = defaultdict(list)
            code = node['params'].get('sql', '').lower()

            # Проверка - участвует ли в ноде хотя бы одна таблица из подрезки
            tables_from_cut_pattern = '|'.join(map(re.escape, tables_from_cut))
            if not (vial_prefix in code or 'test_' in code or '<>_' in code) \
                    and not re.search(tables_from_cut_pattern, code):
                continue

            # Определение перечня таблиц (или алиасов) из ноды
            all_aliases = SqlParser.get_all_table_aliases(code)
            all_aliases_without_tmp = dict()  # только алиасы существующих таблиц (не временные, использующие в даге)
            for k, v in all_aliases.items():
                if k.startswith((vial_prefix, 'test_', '<>_')):
                    new_schema, table_name = set_prefix_in_schema('<>', k)
                    all_aliases_without_tmp[f'{new_schema}.{table_name}'] = v

            cut_table_aliases = []
            for table, alias in all_aliases_without_tmp.items():
                if table in tables_from_cut:
                    cut_table_aliases.append(alias)
            # Если в ноде нет подрезаемых таблиц - переходим к следующей
            if not cut_table_aliases:
                continue
            cut_aliases_pattern = '|'.join(map(re.escape, cut_table_aliases))

            """
            Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
            а в кат effective_to_dttm = '5999-01-01'
            """
            date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
            code = re.sub(date_pattern, r"'\1 00:00:00'", code)

            """
            Убираем префикс с названием таблицы: couchbase_application.dt_updated -> dt_updated
            """
            replace_pattern = r'(\(|,|>|<|=)\s*\w+\.'
            code = re.sub(replace_pattern, r'\1', code)
            # Получение фильтров из json-части
            all_filters = SqlParser.extract_join_filters(code)
            # Находим WHERE-часть запроса
            where_pattern = r'\bwhere\b(.*?)(?:\border by\b|\bgroup by\b|\bhaving\b|\blimit\b|\boffset\b|$)'
            where_match = re.search(where_pattern, code, re.DOTALL)

            if where_match:
                where_clause = where_match.group(1).strip()
                all_filters.append(where_clause)

            # Проверяем, что фильтр принадлежит подрезаемой таблице
            for whole_filter in all_filters:
                if not re.search(cut_aliases_pattern, whole_filter):
                    continue

                # Избавляемся от лишних символов (сравнение двух фильтров происходит без них)
                from helpers.parsers import ChimeraSqlScriptParser
                whole_filter = ChimeraSqlScriptParser.delete_comments_in_script(whole_filter)

                # Из фильтров убираем часть с названием таблица/алиаса
                for table, alias in all_aliases_without_tmp.items():
                    splited_table = table.split('.')
                    table_name = splited_table[1]
                    if whole_filter.startswith(f'{table}.'):
                        whole_filter = whole_filter.replace(f'{table}.', '')
                        whole_filter_with_table[table].append(whole_filter)
                        break
                    elif whole_filter.startswith(f'{table_name}.'):
                        whole_filter = whole_filter.replace(f'{table_name}.', '')
                        whole_filter_with_table[table].append(whole_filter)
                        break
                    elif whole_filter.startswith(f'{alias}.'):
                        whole_filter = whole_filter.replace(f'{alias}.', '')
                        whole_filter_with_table[table].append(whole_filter)
                        break

                for table, whole_filter in whole_filter_with_table.items():
                    table_schema, table_name = set_prefix_in_schema('<>', table)
                    table = f'{table_schema}.{table_name}'
                    split_filters = []
                    for filter in whole_filter:
                        split_filters.extend([cut_filter.strip()
                                              for cut_filter in re.split(split_pattern, filter)])
                    for part_of_filter in split_filters:
                        filter_clear = ''.join([symbol for symbol in part_of_filter
                                                if symbol not in symbols_to_exclude])
                        filter_clear = filter_clear.replace("'@@executiondt@@'date", 'currentdate')
                        filter_clear = filter_clear.replace("'@@executionts@@'date", 'currentdate')
                        filter_clear = filter_clear.replace("'@@executionts@@'timestamp", 'currenttimestamp')
                        filter_clear = filter_clear.replace("'@@executionts@@'", 'currenttimestamp')
                        filter_clear = filter_clear.replace("'@@executiondt@@'", 'currentdate')
                        filter_clear = filter_clear.replace("cast'@@executiondt@@'asdate", 'currentdate')
                        filter_clear = filter_clear.replace("cast'@@executiondt@@'astimestamp", 'currenttimestamp')
                        filter_clear = filter_clear.replace('!=', '<>')

                    if filter_clear not in cut_filters_in_dag[table]:
                        cut_filters_in_dag[table].append(filter_clear)

        with allure.step(step_attachment):
            pass

    with allure.step('Сравнение фильтров'):
        result = defaultdict(list)

        for key, values in cut_filters_in_dag.items():
            if key not in cut_filters_in_settings:
                # Ключ есть только в dict_a
                result[key] = values
            else:
                if set(values) == set(cut_filters_in_settings[key]):
                    # Все элементы совпадают, пропускаем данный ключ
                    continue
                else:
                    # Ключ есть в обоих словарях, фильтруем значения
                    result[key] = [item for item in values if item not in cut_filters_in_settings[key]]
                    # Если все фильтры что есть в даге есть и в кате, то удаляем такой ключ полностью
                    if not result[key]:
                        result.pop(key)

        assert not result, f'Фильтров {result} нет в настройках CUT'


@pytest.mark.integration
@allure.id("5314274")
@allure.feature('Job (Dag)')
@allure.story('Настройки ката в джобе')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: Не совпадают фильтры в настройках cut2 и даге')
@allure.description('Проверяет соответствие настроек CUT2 и фильтров в первых нодах дага')
def test_check_cut_filters_in_dag_for_dlh(package_dag, cut2_service_api, vial_prefix):
    with allure.step("Получаем sql каждой ноды"):
        nodes = package_dag.get_transforms_code()
        cut_filters_in_settings = set()
        cut_filters_in_dag = set()
        symbols_to_exclude = [' ', '(', ')', ':', '-', '_', '/', '\n']

        # Attachment
        step_attachment = f'1. С сервера теди получаем sql-код нод дага.\n' \
                          f'Для виала: https://k8s-vial-master-1.ds.prod-tedi.local/tedi/api/v1/dags/{package_dag.name}\n\n' \
                          f'2. Задаем символы для исключения из кода нод и фильтра ката (symbols_to_exclude):\n' \
                          f'{symbols_to_exclude}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Получаем настройки кат'):
        if vial_prefix.startswith('t'):
            table_settings = [table for table in cut2_service_api['test_api'].get_cut_params(package_dag.name)['tables']
                              if table.get('cut_filter')]
        else:
            table_settings = [table for table in cut2_service_api['dev_api'].get_cut_params(package_dag.name)['tables']
                              if table.get('cut_filter')]

        # Attachment
        step_attachment = f'1. Получаем настройки ката из сервиса Cut2.\n' \
                          f'Для виала: http://preview-etl-cut-dwh.tcsbank.ru/api/v3/jobs/{package_dag.name}\n\n' \
                          f'2. Извлекаем фильтры из структуры "tables" (таблица: фильтры):\n'

        for table in table_settings:
            cut_filter = table.get('cut_filter')
            split_pattern = r'(?i)\b(?:or|and)\b'
            split_filters = [cut_filter.strip() for cut_filter in re.split(split_pattern, cut_filter)]
            cut_filters_in_settings.update(split_filters)

            # Attachment
            step_attachment += f'{table.get("meta_name")}:\n{cut_filter}\n\n'

        # Attachment
        cut_filters_in_settings_attach = '\n'.join(cut_filters_in_settings)
        step_attachment += f'\n3. Разбиваем фильтры на части, разделенные AND и OR. ' \
                           f'Записываем в cut_filters_in_settings:\n' \
                           f'{cut_filters_in_settings_attach}\n\n'
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем фильтры ката'):
        filters_before_removing = dict()
        cut_filters_in_settings_copy = cut_filters_in_settings.copy()
        for filter_before_removing in cut_filters_in_settings_copy:
            filter_after_removing = filter_before_removing.lower()

            """
            Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
            а в кат effective_to_dttm = '5999-01-01'
            """
            date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
            filter_after_removing = re.sub(date_pattern, r"'\1 00:00:00'", filter_after_removing)

            filter_after_removing = ''.join([symbol for symbol in filter_after_removing
                                             if symbol not in symbols_to_exclude])

            """
            В даге может быть current_date, current_timestamp, а в кат now()::date, now()
            """
            filter_after_removing = filter_after_removing.replace('nowdate', 'currentdate')
            filter_after_removing = filter_after_removing.replace('nowtimestamp', 'currenttimestamp')
            filter_after_removing = filter_after_removing.replace('currentdatedate', 'currentdate')
            filter_after_removing = filter_after_removing.replace('now', 'currenttimestamp')
            filter_after_removing = filter_after_removing.replace('!=', '<>')

            cut_filters_in_settings.discard(filter_before_removing)
            cut_filters_in_settings.add(filter_after_removing)
            filters_before_removing[filter_after_removing] = filter_before_removing

        # Attachment
        step_attachment = f'В списке фильтров (cut_filters_in_settings) удаляем из каждого ' \
                          f'фильтра элементы symbols_to_exclude. Заменяем:\n' \
                          f'now()::date на current_date;\n' \
                          f'now()::timestamp на current_timestamp;\n' \
                          f'now() на current_timestamp.\n\n' \
                          f'Фильтры после преобразований:\n'
        step_attachment += '\n'.join(cut_filters_in_settings)
        with allure.step(step_attachment):
            pass

    with allure.step('Преобразуем код каждой ноды'):
        # Attachment
        step_attachment = f'В коде ноды заменяем макросы:\n' \
                          f'"\'@@ execution_dt @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::date" на "current_date";\n' \
                          f'"\'@@ execution_ts @@\'::timestamp" на "current_timestamp".\n' \
                          f'Удаляем символы из массива symbols_to_exclude.\n'

        for node in nodes:
            code = node['params'].get('sql', '').lower()

            """
            Для случаев, когда в даге effective_to_dttm = '5999-01-01 00:00:00',
            а в кат effective_to_dttm = '5999-01-01'
            """
            date_pattern = r"'(\d{4}-\d{2}-\d{2})'"
            code = re.sub(date_pattern, r"'\1 00:00:00'", code)

            """
            Убираем префикс с названием таблицы: couchbase_application.dt_updated -> dt_updated
            """
            replace_pattern = r'(\(|,|>|<|=)\s*\w+\.'
            code = re.sub(replace_pattern, r'\1', code)

            code = ''.join([symbol for symbol in code
                            if symbol not in symbols_to_exclude])
            code = code.replace("'@@executiondt@@'date", 'currentdate')
            code = code.replace("'@@executionts@@'date", 'currentdate')
            code = code.replace("'@@executionts@@'timestamp", 'currenttimestamp')
            code = code.replace("'@@executionts@@'", 'currenttimestamp')
            code = code.replace("'@@executiondt@@'", 'currentdate')
            code = code.replace("cast'@@executiondt@@'asdate", 'currentdate')
            code = code.replace("cast'@@executiondt@@'astimestamp", 'currenttimestamp')
            code = code.replace('!=', '<>')

            node['params']['sql_clear'] = code
            node['cut_filter'] = list()

        with allure.step(step_attachment):
            pass

    with allure.step('Ищем фильтр ката в коде'):
        # Attachment
        step_attachment = f'1. В коде каждой ноды пытаемся найти фильтр из cut_filters_in_settings. Если нашли, ' \
                          f'добавляем в множество cut_filters_in_dag.\n'

        for node in nodes:
            code = node['params'].get('sql_clear', '')
            for cut_filter in cut_filters_in_settings:
                cut_filters_in_dag.add(cut_filter)
                node['cut_filter'].append(cut_filter)

        # Attachment
        filters_not_in_dag = [filters_before_removing[cut_filter] for cut_filter
                              in cut_filters_in_settings - cut_filters_in_dag]

        """
        Если нашли фильтры, которых нет в даге, то проверяем, что в них есть фильтры c in или not in.
        Например: seqno in (0,4,5).
        Т.к. в коде дага может быть три фильтра: seqno=0; seqno=4; seqno=5. Нужно проверить каждый отдельно.
        """
        filters_with_in = list()
        if filters_not_in_dag:
            in_pattern = r'(?i)(\s+|\))(not\s+in|in)\s*\(.*\)'
            filters_with_in = [{'filter_in_settings': cut_filter} for cut_filter in filters_not_in_dag
                               if re.findall(in_pattern, cut_filter)]

        if filters_with_in:
            with allure.step('Разбиваем фильтры ката с in на отдельные фильтры'):
                value_pattern = r"\'.*?\'|\d+"
                field_x_values_pattern = r'(?i)(.+?)\s*(?:not\s+in|in)\s*\((.+)\)'

                """
                Удаляем фильтры с in из filters_not_in_dag.
                Из фильтра получаем поле и значения в секции in.
                """
                for cut_filter in filters_with_in:
                    # удаляем фильтры с in из filters_not_in_dag
                    filters_not_in_dag.pop(filters_not_in_dag.index(cut_filter['filter_in_settings']))

                    in_flg = 'not in' not in cut_filter
                    field, in_values = re.findall(field_x_values_pattern, cut_filter['filter_in_settings'])[0]
                    in_values = re.findall(value_pattern, in_values)
                    in_values = {value: {'found_flg': False, 'filter': ''} for value in in_values}

                    cut_filter['field'] = field
                    cut_filter['in_values'] = in_values
                    cut_filter['in_flg'] = in_flg

                for cut_filter in filters_with_in:
                    """
                    Экранируем скобки, т.к. могут быть такие поля:
                    coalesce(mobile_phone_no,'#') not in ('88888888888', '8888888888')
                    """
                    field = re.sub(r'(\(|\))', r'\\\1', cut_filter['field'])
                    in_or_not_in = 'in' if cut_filter['in_flg'] else r'not\s+in'

                    """
                    Формируем паттерн для поиска фильтров с in в коде дага.
                    """
                    in_node_pattern = fr'(?is){field}\s*{in_or_not_in}\s*\(.*?\)'
                    cut_filter['in_node_pattern'] = in_node_pattern

                    """
                    Создаем фильтры для каждого значения, пример:
                    из balance_group_cd in('NIR','NIP') получаем balance_group_cd='NIR', balance_group_cd='NIP'
                    """
                    operator = '=' if cut_filter['in_flg'] else '<>'
                    for value, value_params in cut_filter['in_values'].items():
                        custom_filter = (field + operator + value).lower()
                        custom_filter = ''.join([symbol for symbol in custom_filter
                                        if symbol not in symbols_to_exclude])
                        value_params['filter'] = custom_filter

                        filters_before_removing[custom_filter] = cut_filter['filter_in_settings']

                # Attachment
                split_filters_attachment = ''
                for cut_filter in filters_with_in:
                    split_filters_attachment += cut_filter['filter_in_settings'] + ':\n\t'
                    split_filters_attachment += '\n\t'.join(
                        value_params['filter'] for value, value_params in cut_filter['in_values'].items())
                    split_filters_attachment += '\n\n'
                allure.attach(split_filters_attachment, name='Разделенные фильтры с in',
                              attachment_type=allure.attachment_type.TEXT)

                """
                Ищем фильтры в даге.
                """
                for node in nodes:
                    code = node['params'].get('sql_clear', '')
                    for cut_filter in filters_with_in:
                        for value_params in cut_filter['in_values'].values():
                            if value_params['filter'] in code:
                                value_params['found_flg'] = True
                                node['cut_filter'].append(value_params['filter'])

                for cut_filter in filters_with_in:
                    """
                    Если не для всех значений нашли фильтры, то ищем в даге фильтры с in.
                    Например в кате один фильтр balance_group_cd in ('NIP', 'NIR', 'NGP'), а в даге два:
                    balance_group_cd in ('NIP', 'NIR') и balance_group_cd in ('NGP').
                    """
                    if not all([value_params['found_flg'] for value, value_params in cut_filter['in_values'].items()]):
                        cut_filter['filters_in_dag'] = set()

                        for node in nodes:
                            code = node['params'].get('sql', '')
                            filters_in_dag = re.findall(cut_filter['in_node_pattern'], code)

                            """
                            Удаляем из фильтров дага символы из symbols_to_exclude, так же как и для ката.
                            """
                            for dag_filter in filters_in_dag:
                                dag_filter_after_removing = ''.join([symbol for symbol in dag_filter
                                                                     if symbol not in symbols_to_exclude])
                                cut_filter['filters_in_dag'].add(dag_filter_after_removing)

                                node['cut_filter'].append(dag_filter_after_removing)

                        """
                        Если нашли в даге фильтры с in, то проверяем есть ли эти значения в фильтрах ката с in.
                        """
                        if cut_filter['filters_in_dag']:
                            cut_filter['values_in_dag'] = set()
                            field = ''.join([symbol for symbol in cut_filter['field']
                                             if symbol not in symbols_to_exclude])
                            field = field + 'in' if cut_filter['in_flg'] else field + 'notin'

                            for dag_filter in cut_filter['filters_in_dag']:
                                values = re.sub(r'(?i)' + field, '', dag_filter)
                                values = values.split(',')
                                cut_filter['values_in_dag'].update(values)

                            for value, value_params in cut_filter['in_values'].items():
                                if value in cut_filter['values_in_dag']:
                                    value_params['found_flg'] = True

                in_filters_not_in_dag = [cut_filter['filter_in_settings'] for cut_filter in filters_with_in if
                                         not all(value_params['found_flg'] for value_params
                                                 in cut_filter['in_values'].values())]
                filters_not_in_dag.extend(in_filters_not_in_dag)

        # Attachment
        for num, node in enumerate(nodes):
            filters = '\n'.join(node.get('cut_filter', list()))
            step_attachment += f'\n#{num + 1}. "{node["task_id"]}". Фильтры:\n{filters}'

        filters_not_in_dag_attach = '\n'.join(filters_not_in_dag)
        step_attachment += f'\n\n2. Ищем фильтры в cut_filters_in_settings, которых нет в ' \
                           f'cut_filters_in_dag.\n\n' \
                           f'Ненайденные фильтры:\n' \
                           f'{filters_not_in_dag_attach}'
        with allure.step(step_attachment):
            pass

        assert filters_not_in_dag == [], 'Найдены фильтры в настройках ката, которых нет в коде дага.'


@pytest.mark.integration
@allure.id("2479096")
@allure.feature('Tables')
@allure.story('ETL зависимости. Зависимые джобы')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'В нодах с группировками в зависимых дагах остались старые названий полей')
@allure.description('Проверяем корректность работы группировки в зависимых дагах после изменения колонок')
def test_group_by_for_dag(depend_dag, tedi_gp_target, change_column, deploy_parser_obj) -> None:
    with allure.step('Получаем sql код из всех нод дага.'):
        transforms = depend_dag.get_transforms_code()

    for num, transform in enumerate(transforms):
        with allure.step('Проверяем ноду: num = {0}, name = {1}'.format(str(num + 1), transform['task_id'])):
            code = transform['params'].get('sql')
            if code is None:
                continue

            group_by_columns = deploy_parser_obj.get_column_without_alias_in_group_by(tedi_gp_target, code, 'GpTable')
            assert change_column not in group_by_columns, "Имя измененной колонки '{0}' совпадает с именем колонки " \
                                                          "зависимого дага в конструкции'group by' без алиаса: {1}."\
                                                           .format(change_column, group_by_columns)


@pytest.mark.integration
@allure.id("2516718")
@allure.feature('Job (Dag)')
@allure.story('Обсолет')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'Даг корректно переведен в obsolete')
@allure.description('1. В репозитории даг находится в папке obsolete. 2. В самом даге локация obsolete. '
                    '3. У дага отсутствует планировщик')
def test_obsol_dags(package_dag, package_dag_by_repo, package) -> None:
    with allure.step('Проверка что в репозитории даг находится в папке obsolete.'):
        assert package_dag_by_repo.in_repo, f'Даг {package_dag_by_repo.name} не найден в репозитории, ' \
                                            f'либо гитлаб недоступен'
        # Используем package_dag_by_repo, т.к. он всегда "смотрит" на ветку по задаче
        package_dag_by_repo.get_repo_path()
        assert 'obsolete/' == package_dag_by_repo.repo_path, f'Даг в репозитории не лежит в папке obsolete/.'

    with allure.step('Проверка меты дага (производится только на этапе разработки).'):
        if not package_dag.meta or package_dag.meta.get('dag_id') is None:
            with allure.step('Даг отсутствует на сервере. Это не ошибка для этапа тестирования'):
                pass
        else:
            with allure.step('Проверка что в самом даге локация obsolete.'):
                location_in_dag = package_dag.meta.get('location')
                assert 'obsolete' == location_in_dag, f'Локация в даге отличается от obsolete.'

            with allure.step('Проверка что дагу выставлен MANUAL планировщик.'):
                scheduler = None
                if package_dag.properties is not None:
                    scheduler = package_dag.properties.get('scheduler', '')

                assert scheduler.upper() == 'MANUAL', f'У дага после обсолета планировщик отличный от MANUAL.'


@pytest.mark.integration
@allure.id("2525527")
@allure.feature('Tables')
@allure.story('Чувствительные данные')
@allure.label('layer', 'integration')
@allure.tag('Static')
@allure.label('title', 'Проверяем, что все поля, с пометкой ЧД на вики сокрыты в репозитори вьюх')
@allure.description('Проверяем объекты из пакета на наличие не скрытых персональных данных')
def test_pd_not_in_restricted_columns(gp_table, wiki_client, vial_prefix, gitlab_views_parser) -> None:
    '''
    Тест, проверяющий объекты из пакета на наличие не скрытых персональных данных.
    '''
    with allure.step('Для таргетов джобов в пакете проверяем наличие пометки "PER" на Wiki'):
        urls = wiki_client.get_potential_dags_wiki_urls(gp_table)

        pd = wiki_client.get_pd_columns(urls)

        allure.attach(urls[0], name='Ссылка объекта на Wiki', attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[0]), name=urls[0])
        allure.attach(urls[1], name='Ссылка объекта на Wiki, если первая оказалась битой',
                      attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link(wiki_client.full_web_url(urls[1]), name=urls[1])
        allure.attach('https://gitlab.tcsbank.ru/dwh-core-public/gpviews/-/tree/master/data',
                      name='Ссылка на репозиторий view',
                      attachment_type=allure.attachment_type.URI_LIST)
        allure.dynamic.link('https://gitlab.tcsbank.ru/dwh-core-public/gpviews/-/tree/master/data',
                            name='Ссылка на репозиторий view')

    with allure.step('Смотрим список атрибутов с персональными данными ищем в файлах restricted_rows.csv/restricted_columns.csv'):
        #считываем содержимое файлов в gitlab
        all_restricted_columns_list = gitlab_views_parser.get_restricted_columns()

        #для проверяемой таблицы выберем множество строк, связанных с этой таблицей,
        # затем нам останется сравнить два множества
        table_name_for_restricted_list = (gp_table.replace('<>_', '<deployment_mode>_v_')).split('.')

         #restricted_columns_check
        columns_in_restricted_columns = set()
        for row in all_restricted_columns_list:
            try:
                if (row[1] == table_name_for_restricted_list[1] and row[0] == table_name_for_restricted_list[0]):
                    columns_in_restricted_columns.add(row[2])

            except:
                pass

        assert columns_in_restricted_columns == pd, f"ЧД на Wiki={pd},  " \
                                                    f"ЧД в gitlab = {columns_in_restricted_columns}" \
                                                    f"Добавьте, пожалуйста, в special " \
                                                    f"инструкцию по сокрытию ЧД или поправьте Wiki!"



@pytest.mark.integration
@allure.id("2605367")
@allure.feature('Конфиг и сценарий')
@allure.story('Run Manual')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Запуск регламентного дага шагом RunManualDag')
@allure.description('Проверяем, что мы шагом RunManualJob не запускаем регламентные джобы')
def test_run_manual_dag_for_reglament(package_scenario_path, scenario_parser):
    '''
    Проверяем, что мы шагом RunManualDag не запускаем регламентные джобы
    '''
    assert_flg = True

    with allure.step('Собираем список дагов, запускаемых шагом RunManualDag'):
        steps = scenario_parser._open_scenario_code(package_scenario_path)
        dags_in_run_manual = []
        if not steps:
            raise NoScenarion("Не можем найти сформированный сценарий наката в Chimera, сделайте Build")
        for key, value in steps.items():
            if value['name'] == 'RunManualDag':
                dags_in_run_manual.append((value['objects'])[0])

    if (dags_in_run_manual):
        with allure.step('Проверяем, что в названиях запускаемых объектов нет объектов без manual'):
            manual_objects = []
            for dag in dags_in_run_manual:
                #проверяем есть ли в названии manual
                if (dag.find('manual') < 0):
                    assert_flg = False

    assert assert_flg, f'На шаге RunManualDag запускается регламентный даг {manual_objects}. ' \
                       f'Обычно так не делают'


@pytest.mark.integration
@allure.id("2666601")
@allure.feature('Tables')
@allure.story('ETL зависимости. Настройки ката зависимых объектов')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'Корректность соответствия имен источников для зависимых дагов с катом')
@allure.description('Проверяем, что таблички, которые участвуют в подрезке у зависимых дагов '
                    'есть в его источниках и настройках CUT')
def test_cut_src_for_dag(depend_dag, tedi_gp_target, cut2_service_api) -> None:
    test_attachment = f'Проверяем корректность соответствия имен источников для зависимых дагов с катом:\n' \
                      f'1 ШАГ: Получаем список источников, которые подвержены кату в зависимом даге с помощью CUT2 API.\n' \
                      f'2 ШАГ: Получаем все источники для зависимого дага.\n' \
                      f'2.1 ШАГ: Проверяем, что таблички, которые участвуют в подрезке - также присутствуют в источниках.\n' \
                      f'2.2 ШАГ: Проверяем, что тестируемая таблица также есть в списке источников зависимого дага.'
    allure.attach(test_attachment, name='АЛГОРИТМ', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Get cut table for dag.'):
        cut2_api = cut2_service_api['prod_api']
        cut_params = cut2_api.get_cut_params(depend_dag.name)
        if cut_params:
            cut_tables = cut_params['tables']
        else:
            cut_tables = []
        cut_tables_full_name = []
        test_attachment = 'Таблицы подрезаемые катом:'
        for cut_table in cut_tables:
            cut_table_schema = cut_table['table_schema'].replace('<>_', '')
            cut_table_name = cut_table['table_name']
            cut_table_full_name = f'{cut_table_schema}_{cut_table_name}'.upper()

            cut_tables_full_name.append(cut_table_full_name)
            test_attachment += f'\n{cut_table_full_name}'
        allure.attach(test_attachment, name='CUT Tables', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Get SRC tables.'):
        sources = depend_dag.get_source_tables()
        test_attachment = 'Таблицы источники:'
        for source in sources:
            test_attachment += f'\n{source}'
        allure.attach(test_attachment, name='Sources', attachment_type=allure.attachment_type.TEXT)

        with allure.step('All cut tables in SRC.'):
            for cut_table_full_name in cut_tables_full_name:
                assert cut_table_full_name in sources, 'Таблица {0} участвует в подрезке в зависимом даге {1}, ' \
                                                       'но отсутствует в его источниках'.format(
                    cut_table_full_name, depend_dag.name)

        with allure.step('Testing table in SRC.'):
            target_meta_name = tedi_gp_target[tedi_gp_target.find('_') + 1:]
            target_meta_name = target_meta_name.replace('.', '_').upper()
            assert target_meta_name in sources, 'Таблица {0} отсутствует среди источников ' \
                                                'зависимого дага {1}.'.format(tedi_gp_target, depend_dag.name)


@pytest.mark.integration
@allure.id("5337626")
@allure.feature('Tables')
@allure.story('ETL зависимости. Настройки ката зависимых объектов')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: Корректность соответствия имен источников для зависимых дагов с катом')
@allure.description('Проверяем, что таблички, которые участвуют в подрезке у зависимых дагов '
                    'есть в его источниках и настройках CUT')
def test_cut_src_for_dag_for_dlh(depend_dag, tedi_dlh_target, cut2_service_api) -> None:
    test_attachment = f'Проверяем корректность соответствия имен источников для зависимых дагов с катом:\n' \
                      f'1 ШАГ: Получаем список источников, которые подвержены кату в зависимом даге с помощью CUT2 API.\n' \
                      f'2 ШАГ: Получаем все источники для зависимого дага.\n' \
                      f'2.1 ШАГ: Проверяем, что таблички, которые участвуют в подрезке - также присутствуют в источниках.\n' \
                      f'2.2 ШАГ: Проверяем, что тестируемая таблица также есть в списке источников зависимого дага.'
    allure.attach(test_attachment, name='АЛГОРИТМ', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Get cut table for dag.'):
        cut2_api = cut2_service_api['prod_api']
        cut_params = cut2_api.get_cut_params(depend_dag.name)
        if cut_params:
            cut_tables = cut_params['tables']
        else:
            cut_tables = []
        cut_tables_full_name = []
        test_attachment = 'Таблицы подрезаемые катом:'
        for cut_table in cut_tables:
            cut_table_schema = cut_table['table_schema'].replace('<>_', '')
            cut_table_name = cut_table['table_name']
            cut_table_full_name = f'{cut_table_schema}_{cut_table_name}'.upper()

            cut_tables_full_name.append(cut_table_full_name)
            test_attachment += f'\n{cut_table_full_name}'
        allure.attach(test_attachment, name='CUT Tables', attachment_type=allure.attachment_type.TEXT)

    with allure.step('Get SRC tables.'):
        sources = depend_dag.get_source_tables()
        test_attachment = 'Таблицы источники:'
        for source in sources:
            test_attachment += f'\n{source}'
        allure.attach(test_attachment, name='Sources', attachment_type=allure.attachment_type.TEXT)

        with allure.step('All cut tables in SRC.'):
            for cut_table_full_name in cut_tables_full_name:
                assert cut_table_full_name in sources, 'Таблица {0} участвует в подрезке в зависимом даге {1}, ' \
                                                       'но отсутствует в его источниках'.format(
                    cut_table_full_name, depend_dag.name)

        with allure.step('Testing table in SRC.'):
            target_meta_name = tedi_dlh_target[tedi_dlh_target.find('_') + 1:]
            target_meta_name = target_meta_name.replace('.', '_').upper()
            assert target_meta_name in sources, 'Таблица {0} отсутствует среди источников ' \
                                                'зависимого дага {1}.'.format(tedi_dlh_target, depend_dag.name)


@pytest.mark.integration
@allure.id("2661640")
@allure.feature('Tables')
@allure.story('Параметры сжатия')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'В таблице есть колонки с типом сжатия, отличным от типа сжатия самой таблицы')
@allure.description('Сравниваем сжатие всей таблицы и сжатие отдельных колонок')
def test_ddl_compression_for_columns(gp_table, vial_prefix):
    '''
    Проверяем, что в таблице нет колонок с типом сжатия, отличным от типа сжатия самой таблицы

    '''

    with allure.step('Получаем тип сжатия таблицы'):
        c = CompareDdl()
        phys_table = vial_prefix+gp_table[2:]

        table_compresstype = ''
        table_orientation = ''
        table_params_str = c.get_compression_for_given_table(phys_table)
        table_params_list = table_params_str[6:-1] if len(table_params_str) > 6 else table_params_str
        table_params_list = table_params_list.split(', ')
        for param in table_params_list:
            param_clean = param.lower().strip()
            if param_clean.startswith('compresstype'):
                table_compresstype = param_clean
            if param_clean.startswith('orientation'):
                table_orientation = param_clean
        allure.attach(table_params_str, name='Данные о таблице из DDL',
                      attachment_type=allure.attachment_type.TEXT)
        allure.attach(table_compresstype, name='Тип сжатия таблицы из DDL',
                      attachment_type=allure.attachment_type.TEXT)

        with allure.step('Получаем способ хранения данных в таблице'):
            allure.attach(table_orientation, name='Способ хранения данных в таблице из DDL',
                          attachment_type=allure.attachment_type.TEXT)

        # завершаем тест, если данные хранятся построчно
        if table_orientation.endswith('row'): return

    with allure.step('Получаем список типов сжатия колонок'):
        cols_compression = c.get_columns_compression_for_given_table(phys_table)
        attachment_to_report = '\n'.join(map(str, cols_compression))
        allure.attach(attachment_to_report, name='Данные о колонках из DDL',
                      attachment_type=allure.attachment_type.TEXT)

    with allure.step('Проверяем отличается ли сжатие колонок от сжатия таблицы'):
        assert_flg = True
        assert_col = []
        for col in cols_compression:
            if table_compresstype not in col[1]:
                assert_flg = False
                assert_col.append(col[0])

        assert assert_flg, f"В таблице {phys_table} есть колонки с другим типом сжатия: {', '.join(assert_col)}. " \
                           f"Проверьте DDL."


@allure.id("3376550")
@pytest.mark.integration
@allure.feature('Job (Dag)')
@allure.story('Настройки ката в даге')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'У дага на кате нет описания')
@allure.description('Проверяет есть ли у дага на кате описание ката в теди')
def test_check_description_cut_dag(package_dag):
    with allure.step("Find cut description."):
        cut_description_in_dag_flg = False
        cut_description = package_dag.meta.get('description')
        cut_valid_descriptions = ['кат', 'cut', 'подрез']
        if cut_description:
            cut_description = cut_description.lower()
            for cut_marker in cut_valid_descriptions:
                if cut_marker in cut_description:
                    cut_description_in_dag_flg = True
        assert cut_description_in_dag_flg, f"Для дага {package_dag.name} на кате не найдено описание ката. " \
                                           f"Ожидаемые значения: {', '.join(cut_valid_descriptions)}"


@pytest.mark.integration
@allure.id("4580991")
@allure.feature('Tables')
@allure.story('Guillotine')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Для таблицы с etl_deleted_flg есть соответствующая гильотина')
@allure.description("""
1. Берем таблицы, у которых имеется поле etl_deleted_flg. 
2. Проверяем наличие настроек гильотины для этих таблиц (в пакете или на проде)
3. Смотрим есть ли в настройках гильотины соответствующий фильтр по этому полю (etl_deleted_flg=1)

Если такого фильтра нет - это ошибка, по стандарту проектирования etl_deleted_flg должны удаляться из таблицы!
""")
def test_there_is_guillotine_for_etl_deleted_flg(gp_table, chimera_api, guillotine_api, package) -> None:
    with allure.step('Проверяем наличие настроек гильотины в пакете или существующей гильотины на Проде'):
        package_files = chimera_api.get_package_files(pack_id=package.name).get('items')
        guillotine_modify = None
        guillotine_on_prod = None
        prod_table = gp_table.replace('<>_', 'prod_', 1)

        for file in package_files:
            guillotine_file_name = 'guillotine_' + gp_table + '.json'
            if guillotine_file_name == file['name']:
                guillotine_file_content = chimera_api.get_package_file_content(package.name, 'gp', file['name'])
                if guillotine_file_content.get('to_modify'):
                    guillotine_modify = guillotine_file_content.get('to_modify')
                    break
        if not guillotine_modify:
            guillotine_on_prod = guillotine_api.get_tasks_info_by_table(prod_table)
        assert guillotine_modify or guillotine_on_prod, f"Настройки гильотины по таблице {prod_table} не найдены."

    with allure.step('Проверяем наличие фильтра etl_deleted_flg=1 у гильотины'):
        check_flg = False
        if guillotine_modify:
            for action in guillotine_modify:
                action_name = action['action_name']
                action_filter = action['action_filter'].replace(' ', '')
                if action_name == 'delete_data' and 'etl_deleted_flg=1' in action_filter:
                    check_flg = True
        elif guillotine_on_prod:
            for action in guillotine_on_prod:
                action_name = action['action_name']
                action_filter = action['action_filter'].replace(' ', '')
                if action_name == 'delete_data' and 'etl_deleted_flg=1' in action_filter:
                    check_flg = True

        error_message = (
            "В настройках гильотины по таблице {prod_table} не обнаружено удаление данных "
            "по фильтру etl_deleted_flg=1, которое требуется по стандарту.\n"
            "P.S. Наличие вьюхи над такой таблицей с фильтрацией по etl_deleted_flg "
            "не освобождает от необходимости настраивать гильотину!!!"
        )

        assert check_flg, error_message.format(prod_table=prod_table)


@pytest.mark.integration
@allure.id("5005452")
@allure.feature('Tables')
@allure.story('Guillotine')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Для таблицы, переводящейся в обсолет, должен быть соответствующий шаг удаления гильотины')
@allure.description('1. Проверяем существование гильотины.'
                    '2. Смотрим есть ли шаг удаления гильотины в сценарии')
def test_deletion_of_guillotine_after_table_to_obsolete(gp_table, package, guillotine_api, chimera_api) -> None:
    with allure.step('Проверяем наличие существующей гильотины на Проде'):
        pref = gp_table.split('_')[0]
        prod_table = gp_table.replace(pref + '_', 'prod_', 1)
        guillotine_on_prod = guillotine_api.get_tasks_info_by_table(prod_table)

    with allure.step('Проверяем наличие шага удаления гильотины в сценарии'):
        if guillotine_on_prod:
            there_is_deletion_of_guillotine_flg = False
            chimera_scenario_steps = chimera_api.get_package_scenario(package.name)
            for step in chimera_scenario_steps:
                if step['step_type'] == ScenarioSteps.DELETE_GUILLOTINE.value:
                    for parameter in step['parameters']:
                        if gp_table in parameter['value']:
                            there_is_deletion_of_guillotine_flg = True
                            break
        else:
            there_is_deletion_of_guillotine_flg = True
        assert there_is_deletion_of_guillotine_flg, f"Для таблицы {gp_table} нет удаления гильотины."


@pytest.mark.integration
@allure.id("5197482")
@allure.feature('Tables')
@allure.story('Guillotine')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Для таблицы, которая стала view, должен быть соответствующий шаг удаления гильотины')
@allure.description('1. Проверяем существование гильотины.'
                    '2. Смотрим есть ли шаг удаления гильотины в сценарии')
def test_deletion_of_guillotine_after_table_to_view(tedi_gp_target, package, guillotine_api, chimera_api) -> None:
    with allure.step('Проверяем наличие существующей гильотины на Проде'):
        pref = tedi_gp_target.split('_')[0]
        prod_table = tedi_gp_target.replace(pref + '_', 'prod_', 1)
        guillotine_on_prod = guillotine_api.get_tasks_info_by_table(prod_table)

    with allure.step('Проверяем наличие шага удаления гильотины в сценарии'):
        if guillotine_on_prod:
            there_is_deletion_of_guillotine_flg = False
            chimera_scenario_steps = chimera_api.get_package_scenario(package.name)
            for step in chimera_scenario_steps:
                if step['step_type'] == ScenarioSteps.DELETE_GUILLOTINE.value:
                    for parameter in step['parameters']:
                        if tedi_gp_target in parameter['value']:
                            there_is_deletion_of_guillotine_flg = True
                            break
        else:
            there_is_deletion_of_guillotine_flg = True
        assert there_is_deletion_of_guillotine_flg, f"Для таблицы {tedi_gp_target} нет удаления гильотины."


@pytest.mark.integration
@allure.id('5091234')
@allure.feature('Job (Dag)')
@allure.story('Настройки лоадеров')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'Указанная в настройках avro-схема существует на Проде')
@allure.description('Проверяет, что указанная в лоадере avro-схема существует на Проде')
def test_existence_of_avro_scheme(tedi_dag, package_dag, schema_registry_api):
    with allure.step('Осуществляем запрос получения avro-схемы'):
        target = package_dag.get_kafka_targets()
        schema_name = target['schema_name']
        schema_registry_prod_api = schema_registry_api['prod_api']
        err_flg, err_mes, schema_registry_prod = schema_registry_prod_api.get_schemas_registry(schema_name)
    with allure.step('Проверка ответа запроса avro-схемы'):
        assert not (err_flg and 'Status code is: 404' in err_mes), f'Схема {schema_name} отсутствует на Проде'
        assert not err_flg, f'Ошибка получения схемы: {err_mes}. Требуется ручная проверка или ретест!'


@pytest.mark.integration_skip
@allure.id("5877248")
@allure.feature('Tables')
@allure.story('Grants GP')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Для источников дага отсутствуют права на проде')
@allure.description('1. Отбираем источники.'
                    '2. Отсеиваем новые + те, по которым раздаются права в пакете'
                    '3. Проверям права таблиц на целевом ETL контуре прода')
def test_grants_for_gp_source_tables(package_dag, package, core_resolver_client, vial_prefix, chimera_api) -> None:
    with allure.step(f'Получаем источники дага {package_dag.name}'):
        from Framework.ETLobjects.table import generate_phys_name_by_meta_name_in_tedi
        source_tables = package_dag.get_source_tables()
        with allure.step(f'Формируем имена физики'):
            phys_tables = [generate_phys_name_by_meta_name_in_tedi(table, vial_prefix) for table in source_tables]
        gp_vial_objects = []
        with allure.step(f'Уточняем имена физики на параметрах vial пробирки'):
            if package.contour in ['chimera_dev', 'chimera_prodlike']:
                vial_id = f'gp.dev.{package.name}'
            else:
                vial_id = f'gp.test.{package.name}'
            for object_vial in chimera_api.get_vial_objects(package.name, vial_id)['items']:
                gp_vial_objects.append(object_vial['name'])

            clear_phys_tables = []
            for phys_table in phys_tables:
                name_to_add = phys_table
                meta_name = '_'.join(phys_table.replace('.', '_').split('_')[1:])
                for vial_table in gp_vial_objects:
                    vial_meta_name = '_'.join(vial_table.replace('.', '_').split('_')[1:])
                    if meta_name == vial_meta_name:
                        vial_table = vial_table.replace('<>', vial_prefix)
                        if phys_table != vial_table:
                            name_to_add = vial_table
                            break
                clear_phys_tables.append(name_to_add)

            # чистим от id_translate
            clear_phys_tables = [table for table in clear_phys_tables if 'utl_md.id_translate' not in table]

    with allure.step('Исключаем новые таблицы'):
        new_tables = package.getMetaObjects('GpTable{n}')
        new_tables = [table.name.replace('<>', vial_prefix) for table in new_tables]

        clear_phys_tables = [table for table in clear_phys_tables if table not in new_tables]

    with allure.step('Определеяем прод ETL и подключаемся к нему'):
        from common.postgresql import GreenPlumSQL
        from Framework.pytest_custom.CustomExceptions import CantDetectAndConnectToProdGP

        gp_clasters = core_resolver_client.get_clusters_and_services()
        host = ''
        port = 5433
        db_name = ''
        for claster in gp_clasters:
            if 'etl' in claster['services']:
                host = claster['address'] + '.tcsbank.ru'
                db_name = claster['db_name']

        if host and port:
            from Config import user
            # from common.proxy_call import wrap_remote
            # gp_prod = wrap_remote(GreenPlumSQL, host, port, db_name, user.GPLogin, user.GPPassword)
            gp_prod = GreenPlumSQL(host, port, db_name, user.GPLogin, user.GPPassword)
        else:
            raise CantDetectAndConnectToProdGP("Не удалось обнаружить хост для прод ETL GP")

    failed_tables = []
    for test_table in clear_phys_tables:
        with allure.step(f"Определеяем права для таблицы {test_table}"):
            table = package.table_factory.generate_table_by_test_name(test_table, 'prod')
            table.set_gp_conn(gp_prod)

            ddl = table.get_ddl()
            if not f"GRANT  SELECT  ON {test_table.replace(vial_prefix, 'prod')} TO gr_etl;" in ddl:
                failed_tables.append(test_table)

    with allure.step("Таблицы, у которых нет прав на ETL"):
        allure.attach(str(failed_tables), name='Список таблиц', attachment_type=allure.attachment_type.TEXT)

        assert not failed_tables, "Для таблиц на проде нет прав SELECT для gr_etl"
