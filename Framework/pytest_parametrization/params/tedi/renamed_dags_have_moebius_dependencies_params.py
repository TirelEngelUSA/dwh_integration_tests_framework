from Framework.pytest_parametrization.params.BaseParam import BaseParametrsBuild


class ParamBuild(BaseParametrsBuild):
    def build(self):
        self.build_base_param('tedi_dag', filter='Dag{!o}')  # для обсолета есть отдельный тест
        self.filter_params(1, self.renamed_dags)

    def renamed_dags(self, dag):
        before_renaming_dags = self.package_analyzer._get_obj_name_before_renaming(dag, 'Dag')
        if before_renaming_dags != dag:
            return False
        return True

