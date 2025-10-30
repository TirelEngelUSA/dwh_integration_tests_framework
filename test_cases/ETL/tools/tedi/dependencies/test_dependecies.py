import pytest
import allure


@pytest.fixture()
def severity_checker():
    def object_checker(obj_name, severity_client):
        with allure.step('Ищем объект таблицы в сервисе Severity'):
            res = severity_client.get_objects_info(obj_name)
            objects = res[1].get('data')
            urn = ''

            if objects:
                for obj_severity in objects:
                    if obj_severity['name'] == obj_name:
                        urn = obj_severity['urn']

        with allure.step("Проверяем наличие SLA в сервисе Severity."):
            if urn:
                relation_sla = []
                checks_url = ''
                res = severity_client.get_relations(urn)
                relations = res[1].get("data")
                if relations:
                    for rel in relations:
                        if rel["obj"]["urn"] == urn and rel["hasActiveSla"]:
                            relation_sla.append(rel)

                with allure.step("Получаем проверки интеграции с DD."):
                    res = severity_client.get_dd_integration_info(urn)
                    checks_info = res[1].get("data")
                    if checks_info['totalSla'] > 0:
                        checks_url = checks_info['link']
                        allure.dynamic.link(checks_url, name=checks_url)

                if relation_sla:
                    allure.attach(str(relation_sla), name='SLA список', attachment_type=allure.attachment_type.TEXT)

                assert not relation_sla, "Найдены SLA на удаляемую таблицу"
                assert not checks_url, "Найдены активные проверки на SLA"

    return object_checker


@pytest.mark.integration
@allure.id("4141298")
@allure.feature('Job (Dag)')
@allure.story('Обсолет')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Даг obsolete имеет зависимости в CRON Moebius')
@allure.description('1. Берем DAG obsolete из пакета. 2. Смотрим зависимости в Moebius CRON')
def test_obsol_dags_have_cron(package_dag, moebius_cron) -> None:
    with allure.step('Получаем информацию о CRON с сервисов Moebius.'):
        cron_relations = moebius_cron.get_scheduled_events()

    with allure.step('Проверка нахождения дага в заданиях cron а.'):
        cron_schedule = None

        for rel in cron_relations:
            urn_entitys = rel['event_urn'].split(':')
            if package_dag.name in urn_entitys:
                cron_schedule = rel
        if cron_schedule:
            moebius_url = moebius_cron.get_main_url()
            allure.dynamic.link(moebius_url, name='Moebius cron API')
            allure.attach(str(cron_schedule), name='Cron event', attachment_type=allure.attachment_type.TEXT)

        assert not cron_schedule, "Найдено задание на Moebius Cron"


@pytest.mark.integration
@allure.id("4141305")
@allure.feature('Job (Dag)')
@allure.story('Обсолет')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Даг obsolete имеет зависимости в child зависимостях Moebius')
@allure.description('''
1. Берем DAG-и obsolete и с планировщиком manual из пакета. 
2. Смотрим зависимости в Moebius Admin
3. Если есть зависимые даги, то выполняем ряд проверок, а именно:
- Если зависимый даг есть в пакете и в него больше не протягиваются поля из обсолет дага, то ОК
- Если зависимый даг обсолетится в этом же пакете, то ОК
- Если не обсолетится, но имеет планировщик manual, то ОК
- Если выключен на Проде в Мебиусе, то ОК
- Если ни одно из условий выше не выполняется - FAILED
''')
def test_obsol_dags_have_moebius_dependencies(package_dag, moebius_admin, package, moebius_admin_swagger) -> None:

    def check_in_obsolete(dag: str) -> bool:
        """ Проверит есть ли даг среди обсолет объектов в этом же пакете """

        obsolete_dags = [dag.Name for dag in package.getMetaObjects('Dag{o}')]

        return dag in obsolete_dags

    def check_is_manual_and_not_in_pack(dag: str) -> bool:
        """ Если даг в пакете, то проверит мануальный ли планировщик у дага и нет ли у него пачки в Мебиусе"""

        if not check_in_obsolete(dag):
            tedi_dag = package.tedi_dag_factory(dag)
            try:
                if tedi_dag.properties['scheduler'] == 'manual':
                    packs = moebius_admin.get_packs(dag)
                    if packs['data']:
                        return False
            except KeyError:
                pass
        return True

    def check_is_not_active_in_moebius(dag: str) -> bool:
        """ Проверит активный ли даг в Мебиусе """

        dag_info_in_moebius = moebius_admin_swagger.get_node(dag)

        return not dag_info_in_moebius['active_flg']

    def check_is_there_in_sources(dag: str) -> bool:
        """ Проверяет остался ли таргет дага в источниках """
        from Framework.ETLobjects.table import generate_meta_name_by_phys_name_in_tedi
        not_obsolete_dags = [dag.Name for dag in package.getMetaObjects('Dag{!o}')]
        for dag in not_obsolete_dags:
            obsolete_tedi_dag = package.tedi_dag_factory(dag)
            sources_of_obsolete_tedi_dag = obsolete_tedi_dag.get_source_tables()
            targets_of_testable_dag = package_dag.get_target_tables()
            for target in targets_of_testable_dag:
                # Приводим имя таргета к формату источников (ddw12345_schema.table -> SCHEMA_TABLE)
                target = generate_meta_name_by_phys_name_in_tedi(target)
                if target in sources_of_obsolete_tedi_dag:
                    return False
            return True

    # Все проверки возвращают True, если зависимый даг выключен в рамках этой проверки
    checks = [check_is_there_in_sources, check_in_obsolete,
              check_is_manual_and_not_in_pack, check_is_not_active_in_moebius]

    with allure.step('Получаем информацию о зависимостях с сервисов Moebius Admin.'):
        relations = moebius_admin.get_node_children(package_dag.name)
    if relations:
        with allure.step('Проверяем актуален ли зависимый даг.'):
            failed_relations = []
            for rel in relations:
                dependent_dag = rel['task_name']
                for check in checks:
                    if check(dependent_dag):
                        break
                else:
                    failed_relations.append(dependent_dag)

        with allure.step('Проверяем наличие связей в Moebius.'):
            if failed_relations:
                allure.dynamic.link(moebius_admin.get_prod_url(), name='Moebius admin API')
                allure.attach(str(failed_relations), name='Child relations',
                              attachment_type=allure.attachment_type.TEXT)

            assert not failed_relations, "Найдены child задания, которых нет в пакете."


@pytest.mark.migration
@pytest.mark.integration
@allure.id("5645298")
@allure.feature('Job (Dag)')
@allure.story('ETL зависимости. Зависимые даги')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Переименованный Даг имеет зависимости в child зависимостях Moebius')
@allure.description('1. Берем переименованный даг из пакета. 2. Смотрим зависимости в Moebius Admin')
def test_renamed_dags_have_moebius_dependencies(package_dag, moebius_admin, package) -> None:
    with allure.step('Получаем информацию о зависимостях с сервисов Moebius Admin.'):
        dag_old_name = package._get_obj_name_before_renaming(package_dag.name, 'Dag')
        relations = moebius_admin.get_node_children(dag_old_name)
    if relations:
        with allure.step('Фильтруем зависимости объектами из пакета.'):
            filtered_relations = []
            obsol_dags = [dag.Name for dag in package.getMetaObjects('Dag{o}')]
            for rel in relations:
                if rel['task_name'] not in obsol_dags:
                    filtered_relations.append(rel)

        with allure.step('Проверяем наличие связей в Moebius.'):
            if filtered_relations:
                allure.dynamic.link(moebius_admin.get_prod_url(), name='Moebius admin API')
                allure.attach(str(filtered_relations), name='Child relations',
                              attachment_type=allure.attachment_type.TEXT)

            assert not filtered_relations, "Найдены child задания, которых нет в пакете."


@pytest.mark.integration
@allure.id("4488589")
@allure.feature('Job (Dag)')
@allure.story('Обсолет')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'Даг obsolete или переименованный присутствует в пачке Moebius')
@allure.description('1. Берем даги obsolete и переименованные из пакета. 2. Смотрим есть ли они  в пачках Moebius')
def test_there_is_obsol_dag_in_moebius_packs(obsoleted_or_renamed_dags, moebius_admin) -> None:
    with allure.step('Проверяем наличие заобсолеченных или переименованных дагов в пачках.'):
        obsoleted_dags_in_packs = dict()
        packs = moebius_admin.get_packs(obsoleted_or_renamed_dags)
        for pack in packs['data']:
            obsoleted_dags_in_packs[pack['pack_name']] = obsoleted_or_renamed_dags

        if obsoleted_dags_in_packs:
            allure.dynamic.link(moebius_admin.get_prod_url(), name='Moebius admin API')
            allure.attach(str(obsoleted_dags_in_packs), name='dict with obsoleted or renamed dag in packs {pack: dag}',
                          attachment_type=allure.attachment_type.TEXT)

        assert not obsoleted_dags_in_packs, "Найдены пачки с дагами, которые в пакете переводятся в обсолет" \
                                            "или переименовываются."


@pytest.mark.integration
@allure.id("4141321")
@allure.feature('Tables')
@allure.story('ETL зависимости. Зависимые даги')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От удаляемой NIFI таблицы есть зависимые DAGи')
@allure.description('Проверяет, есть ли зависимые DAGи, которые зависят от obsolete nifi таблицы')
def test_obsolete_nifi_depend_dags(nifi_table, package) -> None:
    url = "https://dd.tcsbank.ru/"
    allure.dynamic.link(url, name="Link to DD for manual check")
    with allure.step('Получаем зависимости из DD'):
        relations = package.get_all_package_object_relations_from_dd()

        reletions_for_nifi_table = relations.get(nifi_table)

        if reletions_for_nifi_table:
            destinations = reletions_for_nifi_table['destinations'].get('Loads')
        else:
            destinations = None
    with allure.step('Анализируем зависимости'):
        if destinations:
            filtered_relations = []
            obsol_dags = [dag.Name for dag in package.getMetaObjects('Dag{o}')]
            for dest_rel in destinations['objects']:
                if 'urn:dag:tedi:' in dest_rel['urn']:
                    dag_name = dest_rel['urn'].split('urn:dag:tedi:')[-1]
                    if dag_name not in obsol_dags:
                        filtered_relations.append(dag_name)

            assert not filtered_relations, "Найдены DAGи, которые зависят от удаляемой nifi таблицы"

            gp_tables = set()
            obsol_tables = [table.Name for table in package.getMetaObjects('GpTable{o}')]
            for dest_rel in destinations['objects']:
                if 'urn:ph:table:dwh:greenplum:prod_' in dest_rel['urn'] and '_nifi.' not in dest_rel['name']:
                    table = dest_rel['name']
                    if table not in obsol_tables:
                        gp_tables.add(table)

            assert not gp_tables, "Найдены зависимые GP таблицы, которые являются дочерними для nifi таблицы"


@pytest.mark.integration
@allure.id("4193391")
@allure.feature('Tables')
@allure.story('SLA зависимости.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От удаляемой NIFI таблицы есть зависимые SLA')
@allure.description('Проверяет, есть ли зависимые SLA, которые связаны с obsolete nifi таблицей')
def test_obsolete_nifi_depend_sla_by_severity(nifi_table, severity_api, severity_checker) -> None:
    severity_checker(nifi_table, severity_api)


@pytest.mark.integration
@allure.id("4193431")
@allure.feature('Tables')
@allure.story('SLA зависимости.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От удаляемой GP таблицы есть зависимые SLA')
@allure.description('Проверяет, есть ли зависимые SLA, которые связаны с obsolete GP таблицей')
def test_obsolete_gp_depend_sla_by_severity(gp_table, severity_api, severity_checker) -> None:
    table_name = gp_table.replace('<>_', "prod_")
    severity_checker(table_name, severity_api)


@pytest.mark.integration
@allure.id("5294983")
@allure.feature('Tables')
@allure.story('SLA зависимости.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От удаляемой DLH таблицы есть зависимые SLA')
@allure.description('Проверяет, есть ли зависимые SLA, которые связаны с obsolete GP таблицей')
def test_obsolete_dlh_depend_sla_by_severity(dlh_table, severity_api, severity_checker) -> None:
    table_name = dlh_table.replace('<>_', "prod_")
    severity_checker(table_name, severity_api)


@pytest.mark.integration
@allure.id("4263546")
@allure.feature('Tables')
@allure.story('Зависимости от колонок со сменой типа.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'От колонки с измененным типом есть зависимости вне пакета.')
@allure.description('Проверяет, есть ли зависимые объекты, которые связаны с колонкой, у которой изменится тип')
def test_column_change_type(phys_table, changed_column, mg_replica_helper, package) -> None:
    with allure.step("Find urn of changed column."):
        dd_map = package.get_all_package_object_relations_from_dd()
        table_prod = phys_table.replace(package.table_factory.prefix, 'prod')
        urn_of_changed_column = None
        columns_urns = []

        attribs = dd_map.get(table_prod)
        if attribs:
            destinations = attribs.get('destinations')
            if destinations:
                columns = destinations.get('Contains')
                if columns:
                    columns_urns = columns.get('objects')

        if columns_urns:
            for col in columns_urns:
                if table_prod + '.' + changed_column[0] == col['name']:
                    urn_of_changed_column = col['urn']

        if urn_of_changed_column:
            with allure.step(f"Check depend object by urn: {urn_of_changed_column}"):
                depends = mg_replica_helper.get_depend_etl_objects_by_urn(urn_of_changed_column)
            with allure.step(f"Filter objects from Chimera package"):
                dags = [dag.Name for dag in package.getMetaObjects('Dag{}')]
                depends = [dep for dep in depends if dep[0] not in dags]
                text_help = f"""Найдены зависимости от колонки, которая изменила тип
Колонка - {changed_column[0]}
Измененный новый тип - {changed_column[1]} {str(changed_column[2])}"""
                assert not depends, text_help


@pytest.mark.integration
@allure.id("5356476")
@allure.feature('Tables')
@allure.story('Зависимости от колонок со сменой типа.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: От колонки с измененным типом есть зависимости вне пакета.')
@allure.description('Проверяет, есть ли зависимые объекты, которые связаны с колонкой, у которой изменится тип')
def test_column_change_type_for_dlh(phys_table, changed_column, mg_replica_helper, package) -> None:
    with allure.step("Find urn of changed column."):
        dd_map = package.get_all_package_object_relations_from_dd()
        table_prod = phys_table.replace(package.table_factory.prefix, 'prod')
        urn_of_changed_column = None
        columns_urns = []

        attribs = dd_map.get(table_prod)
        if attribs:
            destinations = attribs.get('destinations')
            if destinations:
                columns = destinations.get('Contains')
                if columns:
                    columns_urns = columns.get('objects')

        if columns_urns:
            for col in columns_urns:
                if table_prod + '.' + changed_column[0] == col['name']:
                    urn_of_changed_column = col['urn']

        if urn_of_changed_column:
            with allure.step(f"Check depend object by urn: {urn_of_changed_column}"):
                depends = mg_replica_helper.get_depend_etl_objects_by_urn(urn_of_changed_column)
            with allure.step(f"Filter objects from Chimera package"):
                dags = [dag.Name for dag in package.getMetaObjects('Dag{}')]
                depends = [dep for dep in depends if dep[0] not in dags]
                text_help = f"""Найдены зависимости от колонки, которая изменила тип
Колонка - {changed_column[0]}
Измененный новый тип - {changed_column[1]} {str(changed_column[2])}"""
                assert not depends, text_help


@pytest.mark.integration
@allure.id("4480659")
@allure.feature('Cut')
@allure.story('Откат при настройках guillotine.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'При откате load_params есть активные настройки guillotine, могут быть потери данных.')
@allure.description(
    'Проверяет, что при откате load_params нет активных настроек guillotine, '
    'что может привести к потери данных.'
    '\n'
    'Возможно ложное срабатывание теста при etl_deleted_flg=1 - в этом случае нужно проверить вручную!'
)
def test_cut2_and_guillotine(cut2_file, tedi_dag, package, guillotine_api) -> None:
    with allure.step("Получаем список файлов с настройками CUT2 из пакета"):
        tables_with_guil_settings = set()
        cut2_settings = cut2_file.get_content()
        tables = set()
        tables_settings = cut2_settings['cut_loadparams'].get('tables')
        if tables_settings:
            for table_setting in tables_settings:
                tables.add(table_setting['table_schema'].replace('<>_', 'prod_') + '.' + table_setting['table_name'])

    with allure.step("Получаем настройки guillotine"):
        if tables:
            for table in tables:
                tasks = [
                    task
                    for task in guillotine_api.get_tasks_info_by_table(table)
                    if task['action_name'] in ['drop_partition', 'delete_data']
                ]
                if tasks:
                    tables_with_guil_settings.add(table)

    with allure.step('Проверяем есть ли настройки guillotine, связанные с удалением данных'):
        assert not tables_with_guil_settings, "Мы нашли задачи Guillotine для таблиц с откатом load_params," \
                                              " проверьте возможную потерю данных"


@pytest.mark.migration
@pytest.mark.integration
@allure.id("5678599")
@allure.feature('Tables')
@allure.story('Проверка настроек в MetaHub.')
@allure.label('layer', 'integration')
@allure.tag('Dynamic')
@allure.label('title', 'DLH: проверка, что таблица занесена в MetaHub.')
@allure.description(
    'Проверяет для всех объектов DLH таблиц в пакете '
    'что для них есть записи в сервисе MetaHub.'
    'https://wiki.tcsbank.ru/display/DW/DWH+Core+Metahub'
)
def test_dlh_tables_in_metahub(phys_table, package, metahub_client) -> None:
    table_prod = phys_table.replace(package.table_factory.prefix, 'prod')
    with allure.step(f"Получаем настройки для таблицы {table_prod} из API MetaHub"):
        settings = metahub_client.get_table_settings(table_prod)
        allure.attach(str(settings), name='Настройки из MetaHub', attachment_type=allure.attachment_type.TEXT)

        if not settings:
            with allure.step(f"Получаем настройки для таблицы {table_prod} из файлов в Chimera"):
                from Framework.ETLobjects.chimera_files import ChimeraCollector
                files = ChimeraCollector.get_metahub_settings_files(package.name)
                for file in files:
                    if file.name == f"metahub_{table_prod.replace('prod_', '<>_')}.json":
                        settings = file.get_content()

    with allure.step("Проверяем наполненность настроек"):
        assert settings.get('primary_keys'), "Настройки по таблице отсутствуют в MetaHub, не нашли primary_keys"

