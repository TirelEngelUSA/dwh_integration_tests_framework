test_renamed_dags_have_moebius_dependencies

Проблема/Цель
В случае переименования дага по принципу BIND <--> LOAD (то есть сам таргет не меняется кроме его типа: View/Table) Мебиус теперь корректно подтягивает зависимости. Соответственно подобные случае не должны уже попадать в тестирование.

Описание
Сейчас в тестирование попадают все даги, которые были переименованы. Нужно доработать параметризатор таким образом, что если в даге "переименовывается" только часть имени BIND/LOAD, то такие даги пропускать (не брать в тестирование)

DoD
Переименованные даги по принципу BIND <--> LOAD пропускаются на уровне параметризатора. 

Текст вопроса:
Переименованный Даг имеет зависимости в child зависимостях Moebius [dds_load_bnpl_account]. Видел похожие обращения с обсолетящимся дагом - тест ожидает что последующие будут обсолетиться.
В данном случае есть зависимость от переименованного дага, таргет-таблица (источник для чайлд дага) при этом не переименовывается.
Обязательно ли добавлять чайлд даг в пакет? или можно игнорировать ошибку?
AssertionError: Найдены child задания, которых нет в пакете.
assert not [{'flow_id': 'prod_airflow_tedi_flow', 'task_id': 'urn:task:airflow_tedi:marti_load_ifrs9_bnp_account_c_attr_v', 'task... 5, 'status': 'succeed', ...}, {'critical_flg': False, 'id': 11960200, 'priority': 5, 'status': 'succeed', ...}, ...]}]


def renamed_dags(self, dag):
    before_renaming_dags = self.package_analyzer._get_obj_name_before_renaming(dag, 'Dag')
    if before_renaming_dags != dag:
        # считать BIND и LOAD эквивалентными — если они единственное различие, вернуть True
        if before_renaming_dags.replace('BIND', '__X__').replace('LOAD', '__X__') == dag.replace('BIND', '__X__').replace('LOAD', '__X__'):
            return True
        return False
    return True
