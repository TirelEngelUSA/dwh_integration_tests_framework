import pytest
import os
from typing import List
from Framework.utils import pickle_package_helper
from Framework.ETLobjects.PackageModels import SASpackage
from Framework.ETLobjects.job import JobDetail
from Framework.ETLobjects.table import TableFactory, get_prefix, TableDetail


@pytest.fixture(scope='session', autouse=True)
def package(request) -> SASpackage:

    pack = request.config.option.pack

    def package_teardown():
        pass

    request.addfinalizer(package_teardown)
    # return pickle_package_helper.load(os.path.join(request.config.option.temp_path, request.config.option.task))
    return pack


@pytest.fixture
def job_depend(request, job_name: str) -> JobDetail:
    def job_depend_teardown():
        pass
    request.addfinalizer(job_depend_teardown)
    job = JobDetail(job_name, '', main=False, countour=request.config.option.contour)
    return job


@pytest.fixture
def target(request, table: str) -> TableDetail:
    target_tab = TableFactory.generate_table_by_name_with_lib(table, prefix=get_prefix(request.config.option.contour,
                                                                                       request.config.option.task))
    try:
        target_tab.meta_n = request.config.option.cashed_meta_name[table]
    except KeyError:
        pass

    def target_teardown():
        if target_tab.get_meta_name():
            request.config.option.cashed_meta_name[table] = target_tab.get_meta_name()

    request.addfinalizer(target_teardown)

    return target_tab


@pytest.fixture
def package_table(request, table: str) -> str:
    def package_table_teardown():
        pass
    request.addfinalizer(package_table_teardown)
    return table


@pytest.fixture
def package_scenario_path(request, package_path: str) -> List[str]:
    def scenario_path_teardown():
        pass
    request.addfinalizer(scenario_path_teardown)
    path = os.path.join(package_path, 'scenario.json')
    return path
