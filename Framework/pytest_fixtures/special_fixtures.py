import pytest
import allure
from Framework.ETLobjects.table import get_prefix
from Framework.exceptions import ChimeraApiException


@pytest.fixture(scope='session')
def logger_for_tests():
    import logging
    from Config import logger_conf

    my_adapter = logging.LoggerAdapter(logging.getLogger(logger_conf.logger_name),
                                       {'daemon': logger_conf.daemon_name, 'task': logger_conf.task})
    return my_adapter


@pytest.fixture(scope='session')
def wiki_client(request):
    from Framework.utils.ModelParser.Wiki import WikiClient

    cln = WikiClient()

    def wiki_client_teardown():
        pass

    request.addfinalizer(wiki_client_teardown)

    return cln


@pytest.fixture(scope='session')
def deploy_parser_obj(request):
    from helpers.parsers import deploy_parser

    def deploy_parser_obj_teardown():
        pass

    request.addfinalizer(deploy_parser_obj_teardown)

    parser = deploy_parser

    return parser


@pytest.fixture(scope='session')
def gitlab_views_parser(request):
    from Framework.utils.views_unique import GitlabViewsParse
    from Config import user

    def gitlab_parser_teardown():
        pass

    request.addfinalizer(gitlab_parser_teardown)

    # для gp_views нужен отдельный сервис-аккаунт, пока используем старый токен
    parser = GitlabViewsParse(token=user.Gitlab_token)

    return parser


@pytest.fixture(scope='session')
def table_factory(request):
    from Framework.ETLobjects.table import TableFactory

    def table_factory_teardown():
        pass

    request.addfinalizer(table_factory_teardown)

    table_object = TableFactory()

    return table_object


@pytest.fixture(scope='session')
def sas_script_parser(request):
    from helpers.parsers import SasScriptsParser

    def sas_script_parser_teardown():
        pass

    request.addfinalizer(sas_script_parser_teardown)

    return SasScriptsParser


@pytest.fixture(scope='session')
def scenario_parser(request):
    from helpers.parsers import ScenarioParser

    def scenario_parser_teardown():
        pass

    request.addfinalizer(scenario_parser_teardown)
    if request.config.option.use_chimera == 'yes':
        ScenarioParser.use_chimera = request.config.option.use_chimera
        ScenarioParser.task = request.config.option.task

    return ScenarioParser


@pytest.fixture(scope='session')
def mg(request):
    from dwh_metadata_extractor import MgClient

    mg_client = MgClient()

    def mg_teardown():
        pass

    request.addfinalizer(mg_teardown)

    return mg_client


@pytest.fixture(scope='session')
def mg_v2(request):
    from Config import user
    from Framework.utils.mg_v2_util import MgApiV2

    mg_client = MgApiV2(user=user.ADLogin, passw=user.ADPassword)

    def mg_v2_teardown():
        pass

    request.addfinalizer(mg_v2_teardown)

    return mg_client


@pytest.fixture()
def mg_replica_helper(request, GP_integration):
    from Framework.utils.mg_v2_util import MgReplicaGpHelper
    from Config.feature_store import FeaturesTogles

    # for future usage - GP_prod_integration

    # togle = FeaturesTogles()
    #
    # if togle.dd_replica_usage == 'test':
    #     helper = MgReplicaGpHelper(GP_integration, schema='test_aru')
    # else:
    #     helper = MgReplicaGpHelper(GP_prod_integration)

    helper = MgReplicaGpHelper(GP_integration, schema='test_aru')

    def mg_replica_helper_teardown():
        pass

    request.addfinalizer(mg_replica_helper_teardown)

    return helper


@pytest.fixture(scope='session')
def cut2_service_api(request, contour_short_name):
    from Framework.utils.cut2_util import Cut2Util

    cut2_api_test = Cut2Util()
    cut2_api_prod = Cut2Util(contour='ProductionContour')
    cut2_api_dev = Cut2Util(contour='DevelopmentContour')
    if contour_short_name == 'chimera_prodlike':
        cut2_api_dev = Cut2Util(contour='ProdlikeContour')

    def cut2_service_teardown():
        pass

    request.addfinalizer(cut2_service_teardown)

    return {'test_api': cut2_api_test, 'prod_api': cut2_api_prod, 'dev_api': cut2_api_dev}


@pytest.fixture(scope='session')
def actuality_api():
    from Framework.utils.actuality_util import ActualityApi

    act_api = ActualityApi()

    return act_api


@pytest.fixture(scope='session')
def cut2_diff_helper():
    from helpers.cut2_helper import CUT2Helper as Cut2_helper

    return Cut2_helper


@pytest.fixture(scope='session')
def sql_type_categories(request):
    from helpers.sql_type.sql_types import TypeCategories
    def sql_type_categories_teardown():
        pass

    request.addfinalizer(sql_type_categories_teardown)

    return TypeCategories


@pytest.fixture(scope='session')
def sql_type_comparator(request):
    from helpers.sql_type.type_comparator import SqlTypeComparator
    def sql_type_comparator_teardown():
        pass

    request.addfinalizer(sql_type_comparator_teardown)

    return SqlTypeComparator


@pytest.fixture(scope='function', autouse=True)
def allure_documentation_link(request):
    test_name = request.node.originalname
    wiki_url = 'https://wiki.tcsbank.ru/pages/viewpage.action?pageId=483397105'
    gitlab_url = 'https://gitlab.tcsbank.ru/dwhtst/dwh_integration_tests_framework/-/blob/master'

    module_name, line_num, _ = request.node.location

    allure.dynamic.link(url=wiki_url, name="Тестовая модель")
    allure.dynamic.link(url=f'{gitlab_url}/{module_name}#L{line_num}', name='Код теста в Gitlab')

    yield ''

    for mark in request.node.own_markers:
        if mark.name == 'allure_label' and mark.kwargs['label_type'] == 'title':
            title = mark.args[0] + '[' + request.node.name.split('[')[-1]
            allure.dynamic.title(title)
            break


@pytest.fixture(scope='session')
def vial_prefix(request, contour_short_name):
    vial_prefix = get_prefix(contour_short_name, request.config.option.task)
    return vial_prefix


@pytest.fixture(scope='session')
def vial_dlh_prefix(request, contour_short_name):
    vial_dlh_prefix = get_prefix(contour_short_name, request.config.option.task, pref_type='dlh')
    return vial_dlh_prefix


@pytest.fixture(scope='session')
def contour_short_name(request):
    contr = 'vial'
    if request.config.option.use_chimera == 'yes':
        contr = 'chimera_test'
        if request.config.option.devial == 'yes':
            contr = 'chimera_dev'
            if request.config.option.contour == 'chimera_prodlike':
                contr = 'chimera_prodlike'
    return contr


@pytest.fixture()
def cut_vial_prefix(vial_prefix):
    return vial_prefix + 'cut'


@pytest.fixture(scope='session')
def live_prefix(request):
    contr = 'live'
    if request.config.option.use_chimera == 'yes':
        contr = 'chimera_live'
    live_prefix = get_prefix(contr, request.config.option.task)

    return live_prefix


@pytest.fixture()
def clickhouse_helper(request):
    from Framework.utils.clickhouse_util import ClickHouseClient

    click_client = ClickHouseClient()

    def clickhouse_helper_teardown():
        pass

    request.addfinalizer(clickhouse_helper_teardown)

    return click_client


@pytest.fixture()
def big_tedi_test_util(request):
    from Framework.utils.bigtedi_util import BigTediAPIClient

    api = BigTediAPIClient(contour='TestVialContour')

    def big_tedi_test_util_teardown():
        pass

    request.addfinalizer(big_tedi_test_util_teardown)

    return api


@pytest.fixture(scope='session')
def big_tedi_custom_contour(request):
    contur = 'DevelopmentContour'
    if request.config.option.contour == 'chimera_prodlike':
        contur = 'ProdlikeContour'

    return contur


@pytest.fixture(scope='session')
def big_tedi_custom_util(request):
    from Framework.utils.bigtedi_util import BigTediAPIClient, BigTediLogBuilder
    from Config.special_configs import BigTediConfig

    api_with_config = {'api_class': BigTediAPIClient,
                       'config_class': BigTediConfig,
                       'log_builder': BigTediLogBuilder}

    def big_tedi_custom_util_teardown():
        pass

    request.addfinalizer(big_tedi_custom_util_teardown)

    return api_with_config


@pytest.fixture(scope='session')
def tedi_migration_tools(request):
    """Набор утилит для тестирования миграции
    Клиент для основного prod Tedi
    классы для формирования единого лога по работе DAGа"""
    from Framework.utils.bigtedi_util import BigTediAPIClient, BigTediLogBuilder
    from helpers.tedi_log_build import generate_log

    api = BigTediAPIClient(contour='ProductionContour')

    tools = {'api': api,
             'log_builder': BigTediLogBuilder,
             'generate_log': generate_log}

    def tedi_migration_tools_teardown():
        pass

    request.addfinalizer(tedi_migration_tools_teardown)

    return tools


@pytest.fixture(scope='session')
def chimera_api(request):
    from Framework.utils.chimera_util import ChimeraApi

    api = ChimeraApi()

    def chimera_api_teardown():
        pass

    request.addfinalizer(chimera_api_teardown)

    return api


@pytest.fixture(scope='session')
def guillotine_api(request):
    from Framework.utils.guillotine_util import GuillotineUtil

    api = GuillotineUtil(use_prod=True)

    def guillotine_api_teardown():
        pass

    request.addfinalizer(guillotine_api_teardown)

    return api


@pytest.fixture(scope='session')
def tedi_plugin_api(request):
    from Framework.utils.chimera_util import TediPluginApi

    api = TediPluginApi()

    def tedi_plugin_api_teardown():
        pass

    request.addfinalizer(tedi_plugin_api_teardown)

    return api


@pytest.fixture(scope='session')
def severity_api(request):
    from Framework.utils.severity import DataSeverityClient

    api = DataSeverityClient()

    def severity_api_teardown():
        pass

    request.addfinalizer(severity_api_teardown)

    return api


@pytest.fixture(scope='session')
def devial_tedi_url(request, chimera_api, tedi_plugin_api, package):
    devial_url = ''
    if request.config.option.use_chimera == 'yes' and package.contour in ('chimera_dev', 'chimera_prodlike'):
        from Framework.utils.chimera_util import get_tedi_custom_url
        devial_url = get_tedi_custom_url(chimera_api, package.name)

    return devial_url


@pytest.fixture(scope='session')
def moebius_env_create(request, chimera_api, tedi_plugin_api, package, devial_tedi_url, big_tedi_custom_util,
                       big_tedi_custom_contour, logger_for_tests):
    from Framework.utils.moebius_utils import MoebiusResolverClient, MoebiusToolsClient, MoebiusResolverSetting, \
        MoebiusToolSetting

    tool_id = f"airflow_tedi_tool_{package.name.lower()}"
    flow_id = f"{package.name.lower()}_airflow_tedi_flow"

    from Config import enviroment
    gp_claster = ''
    host = enviroment.local['gp']['Host']
    host_short = host.split('.')[0]
    from Framework.utils.core_resolver_util import CoreResolverApi

    res = CoreResolverApi().get_clusters_and_services()
    for setting in res:
        if setting['address'] == host_short:
            gp_claster = setting['name']

    tool_client = MoebiusToolsClient(MoebiusToolSetting())
    resolver_client = MoebiusResolverClient(MoebiusResolverSetting())
    if '/api/v1' not in devial_tedi_url:
        url_for_moebius = devial_tedi_url + '/api/v1'
    else:
        url_for_moebius = devial_tedi_url
    if '/tedi/' not in url_for_moebius:
        url_for_moebius = url_for_moebius.replace('/api/v1', '/tedi/api/v1')

    conf = big_tedi_custom_util['config_class'](big_tedi_custom_contour, custom_url=devial_tedi_url)
    user = conf.SystemUser
    password = conf.SystemPassword
    logger_for_tests.info(f'Start create tool - {tool_id}')
    res_tool = tool_client.create_tool_instance(tool_id, url_for_moebius, user=user, password=password)
    allure.attach(str(res_tool), name='tool_create_result', attachment_type=allure.attachment_type.TEXT)

    if gp_claster:
        logger_for_tests.info(f'Start create flow - {flow_id}')
        res_flow = resolver_client.create_flow(flow_id, tool_id, gp_claster=gp_claster)
    else:
        logger_for_tests.info(f'Start create flow - {flow_id}')
        res_flow = resolver_client.create_flow(flow_id, tool_id)
    allure.attach(str(res_flow), name='flow_create_result', attachment_type=allure.attachment_type.TEXT)

    from common.configuration import CatapultConfig

    CatapultConfig.custom_flow_id = flow_id

    def moebius_env_create_teardown():
        logger_for_tests.info(f'Clear flow - {flow_id}')
        resolver_client.deactivate_flow(flow_id)
        logger_for_tests.info(f'Clear tool - {tool_id}')
        tool_client.delete_tool_instance(tool_id)

    request.addfinalizer(moebius_env_create_teardown)

    return devial_tedi_url


@pytest.fixture(scope='session')
def catapulta_util_class(request, big_tedi_custom_util, package, moebius_env_create, logger_for_tests,
                         big_tedi_custom_contour):
    import re
    from Framework.utils.catapulta_util import CatapultRunner as runner
    from Config.special_configs import SchedulerConfig
    from common.configuration import CatapultConfig
    from Framework.utils.catapulta_helpers.uploading import run_uploading

    api_classes = {'runner': runner,
                   'SchedulerConfig': SchedulerConfig,
                   'CatapultConfig': CatapultConfig,
                   'upload_to_catapulta': run_uploading,
                   'list_of_dags': []}

    def catapulta_util_class_teardown():
        if moebius_env_create:
            tedi_api_conf = big_tedi_custom_util['config_class'](big_tedi_custom_contour, custom_url=moebius_env_create)
            tedi_api_conf.is_meta_vial = True
        else:
            tedi_api_conf = big_tedi_custom_util['config_class'](big_tedi_custom_contour)
        tedi_client = big_tedi_custom_util['api_class'](big_tedi_custom_contour, config=tedi_api_conf)
        dags_list = request.config.option.objects.split(',')
        err, mess, cat_deployer = run_uploading(dags_list, package.name, 'delete', 'test', tedi_client,
                                                logger_for_tests, 'moebius')
        allure.attach(str(mess), name='delete_info', attachment_type=allure.attachment_type.TEXT)
        if err:
            deploy_match = re.search(r'deploy_id:\s+([0-9]+)\.', str(mess))
            if deploy_match:
                deploy_id = deploy_match.group(1)
                deploy_info = cat_deployer.get_deploy_info(deploy_id)
                allure.attach(deploy_info.to_str(), name='delete_details',
                              attachment_type=allure.attachment_type.TEXT)

    request.addfinalizer(catapulta_util_class_teardown)

    return api_classes


@pytest.fixture(scope='function')
def cut_autoreview_parser():
    from helpers.parsers import ReviewCutParser

    return ReviewCutParser


@pytest.fixture(scope='function')
def moebius_cron():
    from Framework.utils.moebius import MoebiusCronClient

    api = MoebiusCronClient()

    yield api


@pytest.fixture(scope='function')
def moebius_admin():
    from Framework.utils.moebius import MoebiusAdminClient

    api = MoebiusAdminClient()

    yield api


@pytest.fixture(scope='function')
def moebius_admin_swagger():
    from Framework.utils.moebius import MoebiusAdminSwaggerClient

    api = MoebiusAdminSwaggerClient()

    yield api


@pytest.fixture(scope='function')
def hound_api():
    from Framework.utils.hound_util import HoundUtil

    api = HoundUtil()

    yield api


@pytest.fixture(scope='function')
def datasync_backup_dates(request):
    from Framework.utils.datasync_util import DatasyncUtil

    api = DatasyncUtil()

    yield api.get_backup_dates()


@pytest.fixture(scope='session')
def schema_registry_api(request):
    from Framework.utils.schema_registry_util import SchemaRegistryAPI

    schema_registry_api_test = SchemaRegistryAPI(contour='TestContour')
    schema_registry_api_prod = SchemaRegistryAPI(contour='ProductionContour')

    def schema_registry_service_teardown():
        pass

    request.addfinalizer(schema_registry_service_teardown)

    return {'test_api': schema_registry_api_test, 'prod_api': schema_registry_api_prod}


@pytest.fixture(scope='session')
def s3_client(request):
    from common.s3 import S3Client

    api = S3Client()

    return api


@pytest.fixture(scope='session')
def metahub_client(request):
    import allure
    from Framework.utils.meta_hub_util import MetaHubApi

    api = MetaHubApi()
    with allure.step(api.main_url):
        return api


@pytest.fixture(scope='session')
def core_resolver_client(request):
    import allure
    from Framework.utils.core_resolver_util import CoreResolverApi

    api = CoreResolverApi()
    with allure.step(api.main_url):
        return api


@pytest.fixture(scope='session')
def released_packages_from_sdp(GP_integration):
    from Framework.utils.package_utils import PackageUtils
    from Config import enviroment, user
    from common.postgresql import GreenPlumSQL

    gp_connect = GreenPlumSQL(enviroment.local['gp']['Host'],
                          enviroment.local['gp']['Port'],
                          enviroment.local['gp']['DbName'], user.GPLogin, user.GPPassword)

    released_packages = PackageUtils.get_released_packages_from_sdp(gp_connect)
    gp_connect.close()
    return released_packages


@pytest.fixture(scope='session')
def chimera_objects_by_task_from_s3(s3_client):
    from botocore.exceptions import ClientError
    import json
    file_path = f'chimera_objects_by_task/objects_by_task.json'
    try:
        response = s3_client.get_object(file_path)
        cache = json.loads(response['Body'].read().decode("utf-8"))
    except ClientError as e:
        cache = {}

    class Cache:
        def __init__(self, data):
            self.data = data

        def save(self):
            s3_client.put_object(json.dumps(self.data, indent=2, ensure_ascii=False), file_path)

    yield Cache(cache)
