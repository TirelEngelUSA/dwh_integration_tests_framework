import pytest
from helpers.reports.design_template import DesignReport


@pytest.fixture
def design_report(request):

    des_report = DesignReport(task=request.config.option.task)

    def design_report_teardown():
        pass

    request.addfinalizer(design_report_teardown)

    return des_report
