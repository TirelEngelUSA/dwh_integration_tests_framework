import pytest
import allure
import re
import datetime
from collections import defaultdict
from Framework.pytest_custom.CustomExceptions import CheckCut2Manually
from helpers.common import set_prefix_in_schema


@pytest.mark.integration
@allure.id("1874590")
@allure.feature('Job (Dag)')
@allure.story('Корректность обновления лоадпарамов')
@allure.label('layer', 'module')
@allure.tag('Dynamic')
@allure.label('title', 'Значения load_param в логе дага больше, чем load_param в сервисе cut2')
@allure.description('Проверяет корректность обновления load_params при прогоне дага')
def test_dag_cut2_loadparams_update(package_dag, cut2_service_api, vial_prefix, cut_vial_prefix) -> None:
    """
    Проверяет корректность обновления load_params при прогоне джоба:
        1) Get last load_param - вытягиваем настройки для дага из сервиса CUT2 (http://testvl-etl-cut.dwh.tcsbank.ru/api/v2).
        2) Get DAG last log - берем последний прогон дага и его лог.
        3) Find Cut2 work log in dag log - парсим лог и находим взаимодействие с сервисом Cut2.
        4) Check load_params changes - проверяем, что значения в сервисе больше либо равны последнему отработанному логу.
    """
    with allure.step('Get last load_params'):
        load_params_values = dict()
        if vial_prefix.startswith('t'):
            load_params = cut2_service_api['test_api'].get_last_load_params(package_dag.name)
        else:
            load_params = cut2_service_api['dev_api'].get_last_load_params(package_dag.name)

        step_attachment = 'load_params_values (лоадпарамы, полученные через сервис Cut2):\n\n'

        if load_params.data:
            for table in load_params.data:
                format_without_microseconds = '%Y-%m-%dT%H:%M:%S'
                format_with_microseconds = '%Y-%m-%dT%H:%M:%S.%f'
                datetime_len_no_microseconds = 19
                table_name = table.table_schema.replace('<>', vial_prefix) + '.' + table.table_name
                if len(table.load_param) > datetime_len_no_microseconds:
                    load_params_values[table_name] = datetime.datetime.strptime(table.load_param,
                                                                                format_with_microseconds)
                else:
                    load_params_values[table_name] = datetime.datetime.strptime(table.load_param,
                                                                                format_without_microseconds)
                step_attachment += f"{table_name}: {load_params_values[table_name]}\n"

        # Attachment
        if load_params.data is None:
            step_attachment = 'Нет ответа от сервиса Cut2, load_params_values пуст.'
        with allure.step(step_attachment):
            pass

    with allure.step('Get DAG last log.'):
        tasks_with_logs = package_dag.get_last_tasks_logs()

    with allure.step('Find Cut2 work log in DAG log'):
        table_reg = r"\'name\'\:\s+\'(.+)\'"
        schema_reg = r"\'source_schema\'\:\s+\'(.+)\'"
        load_param_set = r"\'load_param_new\'\:\s*datetime\.(datetime\([0-9]+\,\s+[0-9]+\,\s+[0-9]*\,\s+[0-9]+\,\s+[0-9]+(\,\s+[0-9]+)*\))\,*"

        cut2_works = dict()
        for task in tasks_with_logs:
            if task.get('prerun'):
                log = task.get('prerun')
                log_parts = log.split("|INFO|")
                for log_part in log_parts:
                    if "'insert_query':" in log_part:
                        # Attachment
                        step_attachment = 'Часть лога дага с работой cut2 (в prerun):\n\n'
                        step_attachment += log_part
                        with allure.step(step_attachment):
                            pass

                        if "'key_tables'" in log_part:
                            kt_index = log_part.find("'key_tables'")
                            t_index = log_part.find("'tables'")
                            log_part = log_part[t_index: kt_index if kt_index > t_index else -1]
                        match_load_params = re.findall(load_param_set, log_part)
                        if match_load_params:
                            match_tables = re.findall(table_reg, log_part)
                            if match_tables:
                                match_schemas = re.findall(schema_reg, log_part)
                                step_attachment = 'Источники и их лоадпарамы из лога (cut2_works):\n\n'
                                for schema_match, table_name_match, load_param_match \
                                        in zip(match_schemas, match_tables, match_load_params):
                                    table = schema_match + '.' + table_name_match
                                    cut2_works[table] = eval('datetime.' + load_param_match[0])
                                    step_attachment += f"{table}: {cut2_works[table]}\n"

        # Attachment
        with allure.step(step_attachment):
            pass

    with allure.step("Check load_params changes."):
        # Attachment
        step_attachment = 'Сравниваем лоадпарамы в сервисе cut2 (load_params_values) ' \
                          'и в логе (cut2_works) без микросекунд'
        with allure.step(step_attachment):
            pass
        for table in cut2_works:
            with allure.step(f'Проверка наличия LP для таблицы {table}'):
                if not load_params_values.get(table):
                    table = table.replace(cut_vial_prefix, vial_prefix)
                load_params_values_for_table = load_params_values.get(table)
                assert load_params_values_for_table, f' Для таблицы {table} не найден LP, проверьте прогон на подрезке'

            with allure.step(f'Проверка значения LP в CUT2 и логах для таблицы {table}'):

                with allure.step(f'Проверка значения LP в CUT2 {table}'):
                    assert cut2_works.get(table), f"Для таблицы '{table}' не найден LP в CUT2"

                cut2_log_dttm_wo_mcs = cut2_works[table].replace(microsecond=0)
                # Attachment
                step_attachment = f"{load_params_values_for_table} >= {cut2_log_dttm_wo_mcs} ?"
                with allure.step(step_attachment):
                    pass
                assert load_params_values_for_table >= cut2_log_dttm_wo_mcs, \
                    "Load params in log more than load_params in Cut2 service, check DAG work with CUT2"


@pytest.mark.integration
@allure.id("2117451")
@allure.feature('Tables')
@allure.story('Инкремент дагов')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', 'Инкремент после прогона не больше последнего в буферной таблице')
@allure.description('Проверяет, что инкремент джоба после доработки не превышает максимального инкремента из последней буферной таблицы')
def test_increment_size_for_dags(vial_prefix, live_prefix, tedi_dag, tedi_gp_target, GP_integration,
                                 contour_short_name) -> None:
    """
    Тест на проверку инкремента дага.
    Проверяет, что после доработки инкремент не превышает максимального значения из последней буферной таблицы.
    """

    min_size = 100000       # порог для исключения таблиц с маленьким инкрементом
    threshold_value = 0.05  # добавляем запас в 5%
    threshold_limit = 1 + threshold_value
    int_to_str_pattern = '{:,}'
    vial_schema, vial_table_name = set_prefix_in_schema(vial_prefix, tedi_gp_target)
    live_schema, live_table_name = set_prefix_in_schema(live_prefix, tedi_gp_target)
    vial_name = f'{vial_schema}.{vial_table_name}'
    live_name = f'{live_schema}.{live_table_name}'
    if contour_short_name == 'chimera_prodlike':
        with allure.step('Пропускаем для prodlike пробирок'):
            pass
    else:
        with allure.step('Вычисляем размер последнего инкремента в целевой таблице.' +
                         ' Считаем, что это инкремент за 2 дня.' +
                         ' Поскольку бэкапы делаются в пятницу и воскресенье.'):
            vial_size = GP_integration.executeAndReturnLists(
                """
                select count(*) 
                from {0}
                group by processed_dttm 
                order by processed_dttm desc
                """.format(vial_name)
            )[0][0]
            attachment = int_to_str_pattern.format(vial_size)
            allure.attach(vial_name, name='Целевая таблица', attachment_type=allure.attachment_type.TEXT)
            allure.attach(attachment, name='Инкремент в целевой таблице', attachment_type=allure.attachment_type.TEXT)

        if vial_size < 2 * min_size:
            with allure.step(f'Для таблиц с инкрементом меньше {int_to_str_pattern.format(min_size)} в день завершаем тест на этом'): pass
            return    # завершаем тест

        with allure.step('Находим максимальный размер инкремента в буферной таблице'):
            buffer_schema = GP_integration.executeAndReturnLists(
                """
                select max(schemaname) from pg_tables 
                where tablename = '{0}'
                and schemaname like 'b%';
                """.format(vial_table_name)
            )
            assert buffer_schema[0][0] is not None, f"Буферная таблица {vial_name} не найдена."
            buffer_table = buffer_schema[0][0] + '.' + vial_table_name
            allure.attach(buffer_table, name='Буферная таблица', attachment_type=allure.attachment_type.TEXT)

            max_bfr_size = GP_integration.executeAndReturnLists(
                """
                with group_processed as(
                select count(*) as count_gr
                from {0}
                group by processed_dttm)
                select max(count_gr)
                from group_processed
                """.format(buffer_table)
            )[0][0]
            assert max_bfr_size is not None, f"Запрос на инкремент в буферной таблице {buffer_table} вернул пустой результат." \
                                             f"Кажется {buffer_table} пуста. Проверьте: select count(*) from {buffer_table}"
            attachment = int_to_str_pattern.format(max_bfr_size)
            allure.attach(attachment, name='Инкремент в буферной таблице', attachment_type=allure.attachment_type.TEXT)

        with allure.step('Сравниваем размеры инкрементов. Допустимо небольшое превышение инкремента на vial.'):
            allure.attach(f'{threshold_value:.0%}', name='Допустимое превышение', attachment_type=allure.attachment_type.TEXT)
            if max_bfr_size > 0:
                vial_bfr_size_ratio_per_day = (vial_size / 2 / max_bfr_size)
                attachment =  f'{vial_bfr_size_ratio_per_day:.4f}'
                allure.attach(attachment, name='Соотношение инкрементов', attachment_type=allure.attachment_type.TEXT)

                if vial_bfr_size_ratio_per_day < threshold_limit:
                    return  # завершаем тест


        with allure.step('Вычисляем размер последнего инкремента в целевой таблице на live контуре.' +
                         ' Считаем, что это инкремент за 2 дня.' +
                         ' Поскольку бэкапы делаются в пятницу и воскресенье.'):
            try:
                live_size = GP_integration.executeAndReturnLists(
                    """
                    select count(*) 
                    from {0}
                    group by processed_dttm 
                    order by processed_dttm desc
                    """.format(live_name)
                )[0][0]
            except Exception as e:
                with allure.step('Таблица не найдена'): pass
                assert vial_bfr_size_ratio_per_day < threshold_limit, ("Инкремент таблицы {0} в {1:.2f} раз больше, чем " +
                                                                      "максимальное значение в буфере."
                                                                       ).format(vial_name, vial_bfr_size_ratio_per_day)
                return  # завершаем тест
            finally:
                allure.attach(live_name, name='Целевая таблица на live контуре', attachment_type=allure.attachment_type.TEXT)

            attachment = int_to_str_pattern.format(live_size)
            allure.attach(attachment, name='Инкремент в целевой таблице', attachment_type=allure.attachment_type.TEXT)

        with allure.step('Сравниваем размеры инкрементов на vial и live контурах. Допустимо небольшое превышение инкремента на vial.'):
            allure.attach(f'{threshold_value:.0%}', name='Допустимое превышение', attachment_type=allure.attachment_type.TEXT)
            if live_size > 0:
                vial_live_size_ratio_per_day = (vial_size/ live_size)
                attachment =  f'{vial_live_size_ratio_per_day:.4f}'
                allure.attach(attachment, name='Соотношение инкрементов', attachment_type=allure.attachment_type.TEXT)
                assert vial_live_size_ratio_per_day < threshold_limit, ("Инкремент таблицы {0} в {1:.2f} раз больше, чем " +
                                                                        "инкремент таблицы {2}."
                                                                        ).format(vial_name, vial_live_size_ratio_per_day, live_name)


# @pytest.mark.integration
@pytest.mark.skip('В DPQA-2471 будет рефакторинг теста')
@allure.id("5393401")
@allure.feature('Tables')
@allure.story('Права на таблицу')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', 'Owner таблицы gr_owner = gr_adm')
@allure.description('Проверяет, что владельцем таблицы является gr_adm')
def test_table_owner(vial_prefix, tedi_dag, tedi_gp_target, GP_integration, contour_short_name) -> None:
    """
    Тест на проверку владельца таблицы.
    Проверяет, что после доработки на таблице выставлен правильный владелец.
    """
    vial_schema_name, table_name = set_prefix_in_schema(vial_prefix, tedi_gp_target)

    if contour_short_name == 'chimera_prodlike':
        with allure.step('Пропускаем для prodlike пробирок'):
            pass
    else:
        base_query = f"select get_object_grants('{vial_schema_name}.{table_name}');"

        with allure.step(f"Выполняем функцию определения прав: {base_query}"):
            result_from_func = '\n'.join(
                map(lambda row: str(row[0]) if row[0] is not None else '',
                    GP_integration.executeAndReturnLists(base_query)))

        with allure.step("Проверяем владельца"):
            assert 'owner to "gr_adm"' in result_from_func.lower(),\
                f"Owner of {vial_schema_name}.{table_name} is not 'gr_adm'"


@pytest.mark.integration
@allure.id("3725049")
@allure.feature('Views')
@allure.story('Служебные колонки')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', 'Во вью есть колонка processed_dttm')
@allure.description('Проверяет, что во вью есть колонка processed_dttm')
def test_view_columns(vial_prefix, create_view, GP_integration) -> None:
    """
    Тест на проверку колонок у вью.
    Проверяет, что во вью присутствует служебное поле processed_dttm.
    """
    vial_schema_name, view_name = set_prefix_in_schema(vial_prefix, create_view)

    with allure.step(f"Выполняем поиск колонок для: {vial_schema_name}.{view_name}"):
        query = f"""select column_name from information_schema.columns
                    where
                    table_schema = '{vial_schema_name}' and
                    table_name = '{view_name}'
                ;"""
        columns = [str(row[0]) for row in GP_integration.executeAndReturnLists(query)]

    with allure.step("Проверяем наличие processed_dttm"):
        assert 'processed_dttm' in columns, f"Во view {vial_schema_name}.{view_name} нет колонки processed_dttm"


@pytest.mark.integration
@allure.id("3869403")
@allure.feature('Views')
@allure.story('Типы колонки')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', 'Разные типы колонок при создании вью через UNION ALL')
@allure.description('Если во вью колонки с разными типами при запросе ко вью идет приведение типов, '
                    'что сильно замедляет обращение к ней')
def test_bind_dags_with_columns_type(package_dag, GP_integration, vial_prefix) -> None:
    """
    Тест на проверку типов колонок у вью.
    Проверяет, что нет дополнительных приведений типов.
    """
    with allure.step("Получение lineage дага."):
        lineages = package_dag.tedi_api.get_dag_lineage(package_dag.name)
        lineage = lineages.get('lineages', [])
        assert len(lineage) == 1, 'Ошибка получения lineage - ожидаем, что кол-во полученных lineage==1'
    with allure.step('Получение кода дага'):
        dependencies = lineage[0]['dependencies']
        try:
            # В bind даге одна нода, поэтому без лишней логики пытаемся получить код дага
            code = package_dag.transforms[0]['params']['sql']
        except Exception:
            code = ''
        assert code, 'Ошибка получения кода дага'
    with allure.step("Поиск конструкции union all в коде дага."):
        if 'union all' in code.lower():
            with allure.step('Получение типов колонок по источникам дага'):
                # Словарь, который в качестве ключей хранит таблицы, а значения - списки полей.
                tables_and_columns = defaultdict(list)
                # Словарь, в котором помимо полей добавляется еще и их тип
                tables_columns_types = defaultdict(list)
                # Заполнение tables_and_columns
                for column, tables in dependencies.items():
                    for table in tables:
                        schema, table, column = table.split('.')
                        tables_and_columns[f'{schema}.{table}'].append(column)
                # Заполнение tables_columns_types
                for table, columns in tables_and_columns.items():
                    schema, table = table.split('.')
                    # В случае с тестовым контуром - теди возвращает таблицы с префиксом test_
                    phtys_schema = schema.replace('test_', f'{vial_prefix}_', 1)
                    table_columns_types = GP_integration.get_columns_types(phtys_schema, table, columns)
                    for column in table_columns_types:
                        tables_columns_types[f'{schema}.{table}'].append(column)
            with allure.step("Сравнение типов по каждому полю"):
                potential_problem = dict()
                unique_types = defaultdict(set)
                # По каждому полю формируем множество из типов
                for table, columns_types in tables_columns_types.items():
                    for column in columns_types:
                        unique_types[column['name']].add(column['type'])
                # Проверяем, что множество типов по каждому полю содержит только одно значение, иначе это проблема
                for column, types in unique_types.items():
                    if len(types) > 1:
                        potential_problem[column] = types
                assert not potential_problem, f"Найдены поля с разными типами {potential_problem}"


@pytest.mark.integration
@allure.id("2760558")
@allure.feature('Job (Dag)')
@allure.story('Инкремент дагов')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', 'Даг не запускался на полных данных после изменения настроек cut2')
@allure.description('Проверка, что даг не бежал на полных при изменении настроек ката или при миграции')
def test_dag_with_changed_cut_not_run_on_full_data(package_dag, package) -> None:
    '''
    Тест проверяет, что при изменении настроек ката (н-р, добавлении источников) при прогоне не бежит на полных
    '''
    with allure.step("Проверяем по последнему логу, что даг не бежал на полных."):
        tasks_with_logs = package_dag.get_last_tasks_logs()
        for task in tasks_with_logs:
            if task.get('prerun'):
                log = task.get('prerun')
                if log.find('Not found load_param -> first start') == -1:
                    pytest.skip('Даг не бежал на полных.')
                else:
                    with allure.step('Выводим предупреждение о прогоне на полных.'):
                        raise CheckCut2Manually('В даге изменились настройки ката, и даг бежал на полных!\n'
                                                'Проверьте соответствию ТЗ - насколько это необходимо '
                                                'и почему не использовалась Инит-загрузка или откат LP.')


@pytest.mark.integration
@allure.id("3620186")
@allure.feature('Конфиг и сценарий')
@allure.story('Качество данных')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', '0 строк обновлено/удалено/вставлено при выполнении скриптов')
@allure.description('Проверяет, что в последнем логе скрипта инструкциями create/insert/update/delete '
                    'обновилась по крайней мере одна запись')
def test_no_updates_in_chimera_scrips(script_names, package, chimera_api):
    with allure.step('Получение логов деплоя на тест (от последнего к первому).'):
        script_names = script_names if isinstance(script_names, list) else [script_names]
        phrase_to_search = "number of affected rows: 0"
        phrase_of_success_step = 'Step "execute_sql" succeeded'
        step_log_url = 'https://chimera.tcsbank.ru/pipelineLog/{0}/{1}/true'

        pipelines = chimera_api.get_package_pipelines(package.name, location='test',  pipeline_type='deploy-test')

        successful_sql_executed = []
        script_problems = dict()

    with allure.step(f"В логе скриптов ищем \"{phrase_to_search}\"."):
        for pipe in pipelines['items'][::-1]:
            pipe_id = pipe['pipeline_id']
            index_steps = pipe['current_step']['index_num']
            pipe_log = []
            for i in range(index_steps):
                pipe_log = pipe_log + chimera_api.get_pipeline_log(pipe_id, num=str(i + 1))

            with allure.step(f'Лог деплоя (id: {pipe_id}) от {pipe["created_at"]}.'):
                script_step_map = dict()
                script_problems = dict()
                scripts = list(map(str, script_names))
                max_script_ind = -1
                for ind, step in enumerate(pipe_log):
                    if not scripts and step['step_index_num'] > max_script_ind:
                        break

                    for script in scripts:
                        if script not in script_step_map and script in step['message']:
                            with allure.step(f'--- {script} ---\n'
                                             f'{step_log_url.format(pipe_id, step["step_index_num"])}'):
                                script_step_map[step['step_index_num']] = script
                                max_script_ind = max(max_script_ind, step['step_index_num'])
                                scripts.remove(script)
                                break

                    if step['message'] == phrase_to_search:
                        problem_step = pipe_log[ind - 1]
                        problem_statement = problem_step['message']
                        problem_script = script_step_map.get(problem_step['step_index_num'])
                        if problem_script:
                            if problem_statement not in script_problems.setdefault(problem_script, []):
                                with allure.step(problem_statement):
                                    script_problems[problem_script].append(problem_statement)

                    elif step['message'].startswith(phrase_of_success_step):
                        executed_sql = script_step_map.get(step['step_index_num'])
                        if executed_sql and executed_sql not in successful_sql_executed:
                            successful_sql_executed.append(executed_sql)

            if len(scripts) == len(successful_sql_executed):
                break

        assert not script_problems, "SQL-запросы без обновлений указаны в шагах теста."


@pytest.mark.integration
@allure.id("5316766")
@allure.feature('Конфиг и сценарий')
@allure.story('Качество данных')
@allure.label('layer', 'System')
@allure.tag('Dynamic')
@allure.label('title', '0 строк обновлено/удалено/вставлено при выполнении DLH скриптов')
@allure.description('Проверяет, что в последнем логе скрипта инструкциями create/insert/update/delete '
                    'обновилась по крайней мере одна запись')
def test_no_updates_in_chimera_scrips_for_dlh(script_names, package, chimera_api):
    with allure.step('Получение логов деплоя на тест (от последнего к первому).'):
        script_names = script_names if isinstance(script_names, list) else [script_names]
        phrase_to_search = "number of affected rows: 0"
        phrase_of_success_step = 'Step "execute_sql" succeeded'
        step_log_url = 'https://chimera.tcsbank.ru/pipelineLog/{0}/{1}/true'

        pipelines = chimera_api.get_package_pipelines(package.name, location='test',  pipeline_type='deploy-dev')

        successful_sql_executed = []
        script_problems = dict()

    with allure.step(f"В логе скриптов ищем \"{phrase_to_search}\"."):
        for pipe in pipelines['items'][::-1]:
            pipe_id = pipe['pipeline_id']
            index_steps = pipe['current_step']['index_num']
            pipe_log = []
            for i in range(index_steps):
                pipe_log = pipe_log + chimera_api.get_pipeline_log(pipe_id, num=str(i + 1))

            with allure.step(f'Лог деплоя (id: {pipe_id}) от {pipe["created_at"]}.'):
                script_step_map = dict()
                script_problems = dict()
                scripts = list(map(str, script_names))
                max_script_ind = -1
                for ind, step in enumerate(pipe_log):
                    if not scripts and step['step_index_num'] > max_script_ind:
                        break

                    for script in scripts:
                        if script not in script_step_map and script in step['message']:
                            with allure.step(f'--- {script} ---\n'
                                             f'{step_log_url.format(pipe_id, step["step_index_num"])}'):
                                script_step_map[step['step_index_num']] = script
                                max_script_ind = max(max_script_ind, step['step_index_num'])
                                scripts.remove(script)
                                break

                    if step['message'] == phrase_to_search:
                        problem_step = pipe_log[ind - 1]
                        problem_statement = problem_step['message']
                        normalized_statement = (problem_statement or '').strip().lower()
                        if normalized_statement.startswith('query:') and 'select ns.nspname' in normalized_statement:
                            # пропускаем, тут 0 строк оправдан
                            continue
                        problem_script = script_step_map.get(problem_step['step_index_num'])
                        if problem_script:
                            if problem_statement not in script_problems.setdefault(problem_script, []):
                                with allure.step(problem_statement):
                                    script_problems[problem_script].append(problem_statement)

                    elif step['message'].startswith(phrase_of_success_step):
                        executed_sql = script_step_map.get(step['step_index_num'])
                        if executed_sql and executed_sql not in successful_sql_executed:
                            successful_sql_executed.append(executed_sql)

            if len(scripts) == len(successful_sql_executed):
                break

        assert not script_problems, "SQL-запросы без обновлений указаны в шагах теста."
