test_increment_size_for_dags

Проблема/Цель
В случае переименования таблицы ее бэкап ищется по маске с новым именем:

buffer_schema = GP_integration.executeAndReturnLists(
    """
    select max(schemaname) from pg_tables 
    where tablename = '{0}'
    and schemaname like 'b%';
    """.format(table_name)
) 
Описание
Необходимо бэкап получать по имени таблицы ДО переименования

DoD
Бэкап таблицы корректно определяется в случае переименования таблицы
