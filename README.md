
test_no_updates_in_chimera_scrips

Проблема/Потребность
Проверяются все логи, которые есть при выполнении deploy-test, в некоторых из них присутствует текстовка, что обновилось 0 строк, при этом данные логи никакого отношения к выполнению пользовательских скриптов не имеют.

Что нужно сделать
Необходимо проанализировать каким образом можно учитывать только логи, относящие к шагам execute_sql (вероятно логи остальных шагов не нужно парсить, но этот момент тоже лучше обдумать дополнительно).
Если подобная фильтрация возможна, то реализовтаь ее. 

DoD
Ищем проблемы только в логах, относящихся к выполнению пользовательских скриптов, ложные failed отсутствуют.

Текст ошибки:
AssertionError: SQL-запросы без обновлений указаны в шагах теста.
assert not {'main_gp_sql_script.sql': ["query: \n            SELECT ns.nspname\n                 , cl.relname\n            FROM p... '%prt%'\n            AND ns.nspname like E'tdw82122^_%' escape '^'\n            AND cl.relacl is null\n            "]}

Описание проблемы:
Коллеги, привет!
При выполнении integration_tests падает  ошибкой, в файле на который ссылается не предусмотрена операция update.
Помогите пожалуйста разобраться в чём может быть проблема.

[main_gp_sql_script.sql, check_rules.sql, post_deploy.sql]

                    if step['message'] == phrase_to_search:
                        problem_step = pipe_log[ind - 1]
                        problem_statement = problem_step['message']
                        problem_script = script_step_map.get(problem_step['step_index_num'])
                        if problem_script:
                            if problem_statement not in script_problems.setdefault(problem_script, []):
                                with allure.step(problem_statement):
                                    script_problems[problem_script].append(problem_statement)

                    elif step['message'].startswith(phrase_of_success_step):
                        executed_sql = script_step_map.get(step['step_index_num'])
                        if executed_sql and executed_sql not in successful_sql_executed:
                            successful_sql_executed.append(executed_sql)
