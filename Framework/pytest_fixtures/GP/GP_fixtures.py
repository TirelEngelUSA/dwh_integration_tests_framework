import pytest
from Config import enviroment, user
from Config.special_configs import GlobalConfig
from common.postgresql import GreenPlumSQL


@pytest.fixture()
def GP_integration(request) -> GreenPlumSQL:
    gp_api = GreenPlumSQL(enviroment.local['gp']['Host'],
                          enviroment.local['gp']['Port'],
                          enviroment.local['gp']['DbName'], user.GPLogin, user.GPPassword)

    def GP_integration_teardown():
        pass
    request.addfinalizer(GP_integration_teardown)

    return gp_api


@pytest.fixture()
def GP_prod_integration(request) -> GreenPlumSQL:
    g_conf = GlobalConfig()
    gp_prod_conn = GreenPlumSQL(enviroment.local['gp_prod']['Host'], enviroment.local['gp_prod']['Port'],
                                enviroment.local['gp_prod']['DbName'],
                                g_conf.gp_prod_user,
                                g_conf.gp_prod_password)

    def GP_prod_integration_teardown():
        pass
    request.addfinalizer(GP_prod_integration_teardown)

    return gp_prod_conn
