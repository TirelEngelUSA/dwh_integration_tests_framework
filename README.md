
test_ddl_coincide_with_wiki_for_dags

Описание
Падает тест test_ddl_coincide_with_wiki_for_dags из-за того, что у названия поля на вики в конце был добавлен пробел.

Надо проверить где у нас отсутствует вызов strip()

Пример
https://time.tbank.ru/tinkoff/pl/7hz88i6g4bry9mneunjii9n7wa

Сценарий воспроизведения
// Описать при каких обстоятельствах дефект может возникать
