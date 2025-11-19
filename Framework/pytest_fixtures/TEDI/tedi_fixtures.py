import pytest

from Framework.ETLobjects.dag import TediDag, depend_dags_cash


@pytest.fixture
def package_dag_factory(request, big_tedi_custom_util, big_tedi_custom_contour, devial_tedi_url):
    """Основная фабрика получения клинта от окружения 
    :big_tedi_custom_contour: - для выбора токена, для каждого окружения свой
    :devial_tedi_url: - основной урл клиента, который получаем из окружения
    """
    def build_tedi_dag(tedi_dag):
        if tedi_dag.startswith('manual_'):
            pack_dag = TediDag(tedi_dag, branch=request.config.option.task, use_tedi_api=False)
        else:
            if request.config.option.devial == 'yes':
                tedi_api_conf = big_tedi_custom_util['config_class'](big_tedi_custom_contour, custom_url=devial_tedi_url)
                tedi_api_conf.is_meta_vial = True
                pack_dag = TediDag(tedi_dag, contour='DevelopmentContour', custom_config_for_tedi_api=tedi_api_conf)
            else:
                pack_dag = TediDag(tedi_dag)
        return pack_dag
    return build_tedi_dag


@pytest.fixture
def package_dag(request, tedi_dag: str, package_dag_factory) -> TediDag:
    def package_dag_teardown():
        pass
    request.addfinalizer(package_dag_teardown)
    return package_dag_factory(tedi_dag)


@pytest.fixture
def package_dag_by_repo(request, tedi_dag: str) -> TediDag:
    def package_dag_by_repo_teardown():
        pass
    request.addfinalizer(package_dag_by_repo_teardown)
    pack_dag = TediDag(tedi_dag, branch=request.config.option.task, use_tedi_api=False, lazy=False, strong_branch=True)

    return pack_dag


@pytest.fixture
def production_dag(request, package, tedi_dag: str) -> TediDag:
    def prod_dag_teardown():
        pass
    request.addfinalizer(prod_dag_teardown)
    prod_dag = TediDag(package.get_dag_name_before_renaming(tedi_dag), use_tedi_api=False, lazy=False,
                       contour='ProductionContour')

    return prod_dag


@pytest.fixture
def depend_dag(request, depend_tedi_dag: str) -> TediDag:
    def depend_dag_teardown():
        pass
    request.addfinalizer(depend_dag_teardown)
    if depend_dags_cash.get(f'TediDag: {depend_tedi_dag}'):
        dag = depend_dags_cash.get(f'TediDag: {depend_tedi_dag}')
    else:
        dag = TediDag(depend_tedi_dag, use_tedi_api=False, lazy=False)
        depend_dags_cash[str(dag)] = dag

    return dag
