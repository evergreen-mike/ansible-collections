## Описание модулей


## Установка коллекции через ansible-galaxy
`ansible-galaxy collection install git@github.com:clickhouse-collections.git#/modules/`

## Пример обращения к модулям в плейбуке
```
- name: создать БД
    ch.modules.clickhouse_db:
      db_name: test_db
      engine: Atomic
      cluster: my_cluster
```
