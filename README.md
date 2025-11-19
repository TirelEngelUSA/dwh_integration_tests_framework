
test_ddl_coincide_with_wiki_for_dags

Описание
Падает тест test_ddl_coincide_with_wiki_for_dags из-за того, что у названия поля на вики в конце был добавлен пробел.

Надо проверить где у нас отсутствует вызов strip()

Пример
https://time.tbank.ru/tinkoff/pl/7hz88i6g4bry9mneunjii9n7wa

Сценарий воспроизведения
// Описать при каких обстоятельствах дефект может возникать

Разъяснение шагов:

fltr = filter if filter else 'Dag{!o}' — используется фильтр, который передал вызывающий (в нашем случае 'Dag{!n,s}').
dags = [dag.Name for dag in self.package_analyzer.getMetaObjects(fltr)] — запрашиваются мета-объекты (DAg) из пакета через package_analyzer.getMetaObjects(fltr). Это и есть этап «взять список дагов из пакета по фильтру». (Функция getMetaObjects реализована в модели пакета и принимает такие фильтры типа 'Dag{!n,s}' — см. реализацию Package.getMetaObjects / BasePackage.getMetaObjects.)
Для каждого имени дага создаётся экземпляр TediDag через package_analyzer.tedi_dag_factory(dag, branch=...).
tedi_dag_factory на пакете настраивает фабрику TediDag (см. SASpackage.tedi_dag_factory в PackageModels).
Для каждого созданного tech_dag вызывается tech_dag.get_target_tables() — метод TediDag, возвращающий список GP-таргетов дага.
Затем для каждого найденного target формируется кортеж (tech_dag.name, target) и добавляется в params, а для идентификаторов создаётся строка f'{tech_dag}, {target}' и добавляется в ids.
В конце params и ids передаются в __collect_local_params.
Как сохраняются параметры локально (Params)
