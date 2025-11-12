from Framework.pytest_parametrization.params.BaseParam import BaseParametrsBuild


class ParamBuild(BaseParametrsBuild):

    def build(self):
        self.build_base_param('tedi_dag,tedi_gp_target', filter='Dag{!n,s}')
        self.filter_params(1, self.is_manual_dag)
        self.filter_params(1, self.is_mov_dag)
        self.filter_params(1, self.is_bind_dag)

    def is_manual_dag(self, tedi_dag):
        return  tedi_dag.startswith('manual_')

    def is_mov_dag(self, tedi_dag):
        return  tedi_dag.startswith('mov_')

    def is_bind_dag(self, tedi_dag):
        return '_bind_' in tedi_dag
