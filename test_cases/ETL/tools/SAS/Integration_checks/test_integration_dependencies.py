import pytest
import allure
import re
from Framework.pytest_custom.CustomExceptions import *


@pytest.mark.integration
@allure.id("1807709")
@allure.feature('Tables')
@allure.story('Обсолет')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Проверяем, что при обсолете idtr не осталось объектов, которые их используют')
@allure.description('Проверяем, что для данной дропнутой таблицы трансляции не нашлось зависимых джобов')
def test_droped_idt(idt_table, mg_replica_helper, package) -> None:
    with allure.step(f'Ищем даги, использующие удаляемую {idt_table}.'):
        idt_table_dd = idt_table.split('.')[-1].lower()
        dags = set(mg_replica_helper.get_job_uses_id_translate(idt=idt_table_dd))

        if len(dags) == 0:
            pytest.skip(f'{idt_table} не используется  джобах.')

        # Attachment
        dags_to_print = '\n'.join(sorted(dags))
        with allure.step(dags_to_print):
            pass

    with allure.step('Получаем джобы и даги из пакета (до переименования, если было).'):
        dags_from_config = {package.get_dag_name_before_renaming(dag.Name)
                            for dag in package.getMetaObjects(filters='Dag{}')}

        # Attachment
        dags_to_print = '\n'.join(sorted(dags_from_config))
        with allure.step(dags_to_print):
            pass

    with allure.step('Проверяем, что все даги, использующие id_translate, учтены в пакете.'):
        dags_not_in_config = dags - dags_from_config

        if dags_not_in_config:
            # Attachment
            dags_to_print = '\n'.join(sorted(dags_not_in_config))
            step_attachment = f'Неучтенные зависимые:\n{dags_to_print}'
            with allure.step(step_attachment):
                pass

        assert len(dags_not_in_config) == 0, f'Есть даги, зависящие от удаляемой {idt_table}'


@pytest.mark.integration
@allure.id("1807731")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомных вьюх от таблицы с переименнованной/удаленной колонкой')
@allure.description('Проверяет существование кастомной вьюхи, зависящей от таблицы, '
                    'в которой изменился набор полей или их имена')
def test_depend_custom_view_from_table_with_changed_column(table, change_column, gitlab_views_parser, table_factory) -> None:
    with allure.step('Stash parsing.'):
        exception_text = ''
        # views = StashParse()
        all_views = gitlab_views_parser.code_for_all_views()

    with allure.step('View search.'):
        table_obj = table_factory.generate_table_by_meta_name(table)
        correct_phys_name = table_obj.phys_name.replace(table_obj.prefix, '<deployment_mode>')
        # correct_phys_name = '<deployment_mode>_odd.google_report_extensions'

        for key, value in all_views.items():
            if correct_phys_name in value and change_column in value:
                exception_text = 'Columns of the table were dropped or renamed.Please check depending views.'
                url = gitlab_views_parser.gitlab_views_url + key
                allure.dynamic.link(url, name="Link to view: {0}".format(key))
        assert exception_text == ''


@pytest.mark.integration
@allure.id("1807701")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомных вьюх от таблицы с переименнованной/удаленной колонкой')
@allure.description('Проверяет существование кастомной вьюхи, зависящей от таблицы, '
                    'которая использвется в сас скриптах с "ALTER TABLE"')
def test_depend_custom_view_from_table_in_sas_script(change_table, gitlab_views_parser, package, mg_replica_helper) -> None:
    with allure.step('Получаем список дагов(s) из конфига'):
        dags_from_config = {dag.Name for dag in package.getMetaObjects('Dag{s}')}
        dags_to_print = '\n'.join(sorted(dags_from_config))
        with allure.step(dags_to_print):
            pass

    with allure.step('Ищем представления, в которых используется таблица'):
        depend_views = dict()
        for view_link in gitlab_views_parser.get_table_dependent_views(change_table):
            depend_views[view_link] = gitlab_views_parser.get_view_name_by_view_link(view_link)

        depend_views_to_print = '\n'.join(sorted(depend_views.keys()))
        with allure.step(depend_views_to_print):
            pass

    with allure.step('Если для представления существует bind даг, проверяем, что он есть в конфиге'):
        exception_text = ''
        for view_link, view_name in depend_views.items():
            etl_process = mg_replica_helper.get_etl_process_loads_target(view_name)
            bind_name = etl_process[0][0] if etl_process else ''
            if bind_name not in dags_from_config:
                url = gitlab_views_parser.gitlab_views_url + view_link
                allure.dynamic.link(url, name=f"Link to view: {view_link}")
                exception_text = f'Над таблицей {change_table}, которая меняется в скрипте, есть представления. ' \
                                 f'Необходимо проверить зависимости от таблицы. ' \
                                 f'Ссылки на представления в GitLab прикреплены к тесту.'
        assert not exception_text


@pytest.mark.integration
@allure.id("1807704")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие кастомных вьюх над таблицами с измененной колонкой')
@allure.description('Проверяет отсутствие зависимостей в BIND джобах')
def test_depend_bind(depend_tedi_dag, tedi_gp_target, change_column) -> None:
    raise DependInBindJob('In job - {0}, pls check table - {1}, with column - {2}'.format(depend_tedi_dag,
                                                                                          tedi_gp_target,
                                                                                          change_column))


@pytest.mark.integration
@allure.id("5315899")
@allure.feature('Tables')
@allure.story('ETL зависимости. Кастомные вьюхи')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: Наличие кастомных вьюх над таблицами с измененной колонкой')
@allure.description('Проверяет отсутствие зависимостей в BIND джобах')
def test_depend_bind_for_dlh(depend_tedi_dag, tedi_dlh_target, change_column) -> None:
    raise DependInBindJob('In job - {0}, pls check table - {1}, with column - {2}'.format(depend_tedi_dag,
                                                                                          tedi_dlh_target,
                                                                                          change_column))


@pytest.mark.integration
@allure.id("1807714")
@allure.feature('Конфиг и сценарий')
@allure.story('Шаги')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Дублируется шаг MetadataToObsolete')
@allure.description('Проверка дублей шага в сценарии')
def test_scenario_doubles(package, package_scenario_path, scenario_parser) -> None:
    with allure.step('Get scenario steps'):
        exception_text = ''
        steps = scenario_parser.get_scenario_steps(package_scenario_path)

    with allure.step('Find doubles in scenario'):
        counter = {}
        for elem in steps:
            counter[elem] = counter.get(elem, 0) + 1
            doubles = {element: count for element, count in counter.items() if count > 1}
        if 'MetadataToObsolete' in doubles:
            exception_text = 'Найден лишний шаг MetadataToObsolete'
        assert exception_text == ''


@pytest.mark.integration
@allure.id("1807767")
@allure.feature('Конфиг и сценарий')
@allure.story('Шаги')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Shedule после Rebuild')
@allure.description('Проверяем, что шаг Shedule расположен после Rebuild Shedule')
def test_scenario_schedule_sequence(package, package_scenario_path, scenario_parser) -> None:
    with allure.step('Get scenario steps'):
        exception_text = ''
        steps = scenario_parser.get_scenario_steps(package_scenario_path)

    if 'RebuildScheduler' in steps:
        with allure.step('Locate schedule steps'):
            a = steps.index('RebuildScheduler')
            b = steps.index('Schedule')
            if b < a:
                exception_text = 'Шаг Schedule расположен в сценарии до шага RebuildScheduler'
            assert exception_text == ''


@pytest.mark.integration
@allure.id("1807702")
@allure.feature('Конфиг и сценарий')
@allure.story('Таблицы')
@allure.label('layer', 'integration')
@allure.tag('Static')
@allure.label('title', 'Не бэкапится изменяемая таблица')
@allure.description('Проверяет наличие изменяемой в скриптах таблицы в конфиге пакета')
def test_modify_table_in_config(package, table_in_script):

    with allure.step('Get all backup / new tables from config / renamed tables from config'):
        tables_in_config = [table.Name for table in package.getMetaObjects('GpTable{}')]
        if table_in_script.startswith('test'):
            table_in_script = table_in_script.replace('test', '<>', 1)
        else:
            assert not table_in_script, f'Выражение {table_in_script} или ошибочно распозанано как имя таблицы,' \
                                        f' или используется недопустимый префикс в скриптах'

        assert table_in_script in tables_in_config, "Table {0} not in config. Please check manually creating a " \
                                                      "table backup in scripts (for id_translate and tables not in " \
                                                      "GP). Maybe this table is temporary.".format(table_in_script)


@pytest.mark.integration
@allure.id("5285378")
@allure.feature('Конфиг и сценарий')
@allure.story('Таблицы')
@allure.label('layer', 'integration')
@allure.tag('Static')
@allure.label('title', 'DLH: Не бэкапится изменяемая таблица')
@allure.description('Проверяет наличие изменяемой в скриптах таблицы в конфиге пакета')
def test_modify_table_in_config_for_dlh(package, table_in_script):
    with allure.step('Get all backup / new tables from config / renamed tables from config'):
        tables_in_config = [table.Name for table in package.getMetaObjects('DlhTable{}')]
        if table_in_script.startswith('test'):
            table_in_script = table_in_script.replace('test', '<>', 1)
        else:
            assert not table_in_script, f'Выражение {table_in_script} или ошибочно распозанано как имя таблицы,' \
                                        f' или используется недопустимый префикс в скриптах'

        assert table_in_script in tables_in_config, "Table {0} not in config. Please check manually creating a " \
                                                      "table backup in scripts (for id_translate and tables not in " \
                                                      "DLH). Maybe this table is temporary.".format(table_in_script)


@pytest.mark.integration
@allure.id("2663300")
@allure.feature('Скрипты')
@allure.story('Таблицы')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Операция drop column осуществляется без cascade')
@allure.description('Проверяет наличие cascade после удаления таблиц в скриптах.')
def test_drop_column_cascade_in_scripts(drop_column) -> None:
    with allure.step('Check all drop columns in scripts have cascade.'):
        if drop_column[1] == 'No':
            condition = 'No cascade'
        else:
            condition = drop_column[1]
        assert condition.lower() == 'cascade', "Table {0} in script {1} has no cascade. Contact the developer and " \
                                               "ask to add a cascade after " \
                                               "drop column.".format(drop_column[0], drop_column[2])


@pytest.mark.integration
@allure.id("2683797")
@allure.feature('Скрипты')
@allure.story('UDD')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'В скриптах нет символов кириллицы в UDD кодах')
@allure.description('Собираем все коды UDD из скриптов и проверяем на наличие символов кириллицы')
def test_cyrillic_in_code_udd_in_scripts(udd_code) -> None:
    with allure.step('Check cyrillic in code UDD function in scripts.'):
        if re.search('[А-Яа-яёЁ]+', udd_code):
            condition = 'Cyrillic'
        else:
            condition = 'okay'
        assert condition.lower() == 'okay', f"Code {udd_code} in script has cyrillic."


@pytest.mark.integration
@allure.id("1807726")
@allure.feature('Конфиг и сценарий')
@allure.story('Сценарий')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Нумерация шагов сценария с 0')
@allure.description('Проверяем, что номер шага и подшага не начинаются на ноль. '
                    'В противном случае, при перезапуске с какого-то подшага запуск будет осуществляться с 0 шага.')
def test_zero_in_scenario_steps(package_scenario_path, scenario_parser):
    with allure.step('Check zeros in scenario steps'):
        steps = scenario_parser._open_scenario_code(package_scenario_path)
        for y in steps:
            assert str(y) != '0'
            zero = re.findall(r'0.\d{1,3}', str(y))
            assert str(y) != str(*zero)
            zero1 = re.findall(r'\d{1,3}.0\d{,3}', str(y))
            assert str(y) != str(*zero1)


# TODO gроанализировать и удалить, если не актуален.
@pytest.mark.integration_exclude
@allure.id("1874573")
@allure.feature('Таблицы')
@allure.story('DataSync')
@allure.label('layer', 'system')
@allure.tag('Dynamic')
@allure.label('title', 'Проверяет, что у подрезаемых датасинком таблиц реальные типы для максимальных значений '
              'на тесте не меньше чем на проде')
@allure.description('Сравниваем информацию об именах колонок таблицы, их типах и категориях типов в буфере и на проде')
def test_type_of_max_val_compare_with_prod(GP_integration, GP_prod_integration, sql_type_categories,
                                           sql_type_comparator, tables_cut_by_datasync):
    with allure.step('Get a list of tables cut by the datasynk for the task'):
        sync_tables = tables_cut_by_datasync

    buff = []

    for item in sync_tables:
        with allure.step(f"Get data for a table '{item.prod_name}'"):
            with allure.step('Get information about columns, types, and type categories'):
                item.add_info_about_column_names_and_types(gp_conn=GP_integration,
                                                           type_categories=[sql_type_categories.NUMERIC,
                                                                            sql_type_categories.CHARACTER])

            with allure.step('Get information about maximum values and their real types in the buffer'):
                item.add_info_about_max_val_on_buffer(gp_conn=GP_integration)

            with allure.step('Get information about maximum values and their real types on the prod'):
                item.add_info_about_max_val_on_prod(gp_conn=GP_integration,
                                                    full_tablename='test_wrk.info_about_max_val_on_prod')

            with allure.step('Compare types'):
                diff = sql_type_comparator.get_info_on_non_matching_real_types_at_max_val(table=item)

                if diff:
                    buff.extend(diff)

    with allure.step('Check that the types in the buffer are not less than on the prod'):
        report_data = sql_type_comparator.get_report_on_columns_with_different_types(data=buff, format='TEXT')
        allure.attach(report_data, 'Report', allure.attachment_type.TEXT)

        if buff:
            raise CheckTypesOfMaxVal('Found columns whose type in the buffer is less than on the prod')


@pytest.mark.integration
@allure.id("1910551")
@allure.feature('Конфиг и сценарий')
@allure.story('Обсолет')
@allure.label('layer', 'module')
@allure.tag('Static')
@allure.label('title', 'Попытка восстановить вью для удаленного объекта')
@allure.description('Проверяет, есть ли объекты из шага RemoveMetadata в шаге RestorationView')
def test_objects_in_scenario_step_restoration_view(package_scenario_path, scenario_parser):
    with allure.step('Get objects in scenario steps RemoveMetadata and RestorationView'):
        steps = scenario_parser._open_scenario_code(package_scenario_path)
        remove_metadata_objects = []
        restoration_view_objects = []
        if not steps:
            raise NoScenarion("Не можем найти сформированный сценарий наката в Chimera, сделайте Build")
        for key, value in steps.items():
            if value['name'] == 'RemoveMetadata':
                remove_metadata_objects = value['objects']
            if value['name'] == 'RestorationView':
                restoration_view_objects = value['objects']

    with allure.step('Check objects in scenario step RestorationView'):
        exception_objects = [obj for obj in remove_metadata_objects if obj in restoration_view_objects]
        assert exception_objects == [], f'Step RestorationView has objects ' \
                                        f'whose meta has been removed: {exception_objects}'


@pytest.mark.integration
@allure.id("2526425")
@allure.feature('Конфиг и сценарий')
@allure.story('Обсолет')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Наличие зависимойстей для obsolete объекта')
@allure.description('Дополнительно проверяем, что у заобсолеченного объекта нет зависимых')
def test_objects_in_scenario_step_drop_views_obsol(object_dvo, package, gitlab_views_parser, mg_replica_helper):
    with allure.step('Собираем данные'):
        expected_result = []
        dependencies = []
        all_views = gitlab_views_parser.get_all_views_names()
        config_objects = [dag.Name for dag in package.getMetaObjects('Dag{i}')] + \
                         [job.Name for job in package.getMetaObjects('Job{!n}')]

        # Пришлось вытаскивать второй раз, так как если у нас 1 объект в шаге, то он в параметрах к тесту передается
        # как строка, а не список
        object_dvo = package.get_scenario_objects('DropViewsOnObsolete')

    with allure.step('Проверяем, что в шаге DropViewsOnObsolete нет вьюх'):
        for view in object_dvo:
            if view in all_views:
                expected_result.append(view)
            assert expected_result == [], f"В шаге DropViewsOnObsolete присутствуют вью : {expected_result}."

    with allure.step('Получаем информацию о зависимых объектах из Data Detective.'):
        expected_result = []
        for target in object_dvo:
            db_target = target.lower().split()
            if db_target[0] == 'dv':
                db_target[0] = 'datavault'
            res = mg_replica_helper.get_urn_of_object('prod_' + db_target[0] + '.' + '_'.join(db_target[1:]),
                                                      type='TABLE')
            if res:
                urn, table_phys = res[0]
            else:
                urn = None
            if urn:
                url = 'https://dd.tcsbank.ru/{0}'.format(urn)
                allure.dynamic.link(url, name="Link to DataDetective: {0}".format(target))
                dependencies = mg_replica_helper.get_depend_etl_objects_by_urn(urn)

        with allure.step('Проверяем есть ли зависимые объекты.'):
            for depend_object in dependencies:
                if depend_object[0] not in config_objects:
                    expected_result.append(depend_object)
            assert expected_result == [], f"У таргета {target} который переводится в обсолет есть зависимые" \
                                          f" объекты: {expected_result}."
