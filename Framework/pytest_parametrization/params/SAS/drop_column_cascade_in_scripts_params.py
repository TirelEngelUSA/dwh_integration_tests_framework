from Framework.pytest_parametrization.params.BaseParam import BaseParametrsBuild
from helpers.parsers import SasScriptsParser, ChimeraSqlScriptParser


class ParamBuild(BaseParametrsBuild):

    def build(self):
        self.add_custom_param('drop_column', self.list_of_drop_columns, '0')

    def list_of_drop_columns(self):
        ids = []
        drop_columns = []

        try:
            SasScriptsTables = SasScriptsParser.find_all_drop_columns_from_list_of_scripts(
                    self.package_analyzer.sas_scripts)
            chimera_scripts_tables = ChimeraSqlScriptParser.find_all_drop_columns_from_list_of_scripts(
                self.package_analyzer.chimera_sql_scripts)

            tables = SasScriptsTables | chimera_scripts_tables
            for table in tables:
                if table and len(table) > 1:
                    d = []
                    for j in table:
                        d.append(j)
                    drop_columns.append(d)

            for i in drop_columns:
                if i[1] is None:
                    i[1] = 'No'
            drop_columns.sort()
            for table in drop_columns:
                ids.append('{0}, {1}'.format(table[0], table[2]))
        except Exception as e:
            pass

        return ids, drop_columns
