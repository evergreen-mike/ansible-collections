#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_pgcol
short_description: создание коллекций кред named_collections в clickhouse для подключения к внешним базам данных postgresql
options:
    login_user:
        description:
            имя пользователя к сессии на сервере clickhouse,
            по умолчанию подключение пользователем default
        required: false
        type: str
    login_password:
        description:
            пароль пользователя для подключения к сессии на сервере clickhouse,
            по умолчанию не задан
        required: false
        type: str
    port:
        description:
            порт для подключения к сессии на сервере clickhouse,
            по умолчанию используется 8123
        required: false
        type: int
    host:
        description:
            хост для подключения к сессии на сервере clickhouse,
            по умолчанию используется 'localhost'
        required: false
        type: str
    collection:
        description:
            имя создаваемой коллекции кред
        requeried: true
        aliases: [name]
        type: str
    check:
        description:
            прверить, существует ли указанная коллекция на сервере clickhouse.
            Используется только с параметром collection.
            Возвращает результат в __имя_переменной__.exists в виде булевого значения.
        default: false
        type: bool
    state:
        description:
            если состояние установлено 'present'(по умолчанию), то указанная коллекция будет создана,
            если установлено 'abscent', то указанная коллекция будет удалена.
        default: present
        choices: [abscent, present]
        type: str
    cluster:
        description:
            название кластера clickhouse, на котором будут выполнены операции. Если не указан,
            то операции будут выполнены только на целевой ноде, указанной при запуске ansible-playbook.
        requeried: false
        type: str
    pg_user:
        description:
            имя пользователя на сервере postgresql, под которым будет осуществляться подключение из clickhouse.
            Пользователю должны быть выданы все необходимые права на объекты в postgresql.
        requeried: false
        type: str
    pg_pswd:
        description:
            пароль пользователя на сервере postgresql, под которым будет осуществляться подключение из clickhouse
        requeried: false
        type: str
    pg_host:
        description:
            хост сервера postgresql, к которому будет осуществляться подключение из clickhouse
        requeried: false
        type: str
    pg_port:
        description:
            порт на котором работает postgresql, к которому будет осуществляться подключение из clickhouse
        default: 5432
        type: str
    pg_db:
        description:
            имя БД на сервере postgresql, к которой будет осуществляться подключение из clickhouse
        requeried: false
        type: str
    pg_schema:
        description:
            имя схемы в "pg_db" на сервере postgresql, откуда будет осуществлять считывание данных.
            Пользователь clickhouse в случае необходимости может сам переопределить схему в движке запроса.
        requeried: false
        type: str
'''

EXAMPLES = r'''
- name: создание коллекции для подключения к БД test на сервере 10.129.0.36
    clickhouse_pgcol:
      collection: test_collection
      cluster: my_cluster
      pg_user: bi_reader
      pg_pswd: '1111'
      pg_host: '10.129.0.36'
      pg_port: 5433
      pg_db: 'test'

- name: удаление существующей коллекции
    clickhouse_pgcol:
      collection: test_collection
      cluster: my_cluster
      state: abscent
'''

RETURN = r'''
changed:
    description:
        статус, указывающий произошли ли изменения в результате выполнения операции
    returned: success
    type: bool
msg:
    description:
        короткое сообщение, указывающее по произошедшие изменеия
    returned: success
    type: str
'''

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native
from clickhouse_connect import get_client


def is_collection_exist(ch_client, collection):
    return {"exists": ch_client.command(f"SELECT count(*) FROM system.named_collections WHERE name = '{collection}'") > 0}


def create_collection(ch_client, module, collection, cluster, pg_user, pg_pswd, pg_host, pg_port, pg_db, pg_sch):
    if is_collection_exist(ch_client, collection)["exists"]:
        query_start = "ALTER"
        query_com = " SET"
        result_msg = "changed"
    else:
        query_start = "CREATE"
        query_com = " AS"
        result_msg = "created"
    query = query_start + f" NAMED COLLECTION {collection}"
    if cluster:
        query += f" ON CLUSTER {cluster}"
    query += query_com + f" user = '{pg_user}', password = '{pg_pswd}', host = '{pg_host}', port = {pg_port}, database = '{pg_db}'"
    if pg_sch:
        query += f", schema = '{pg_sch}'"
    #raise Exception(query)
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Named collection '{collection}' {result_msg}"}


def drop_collection(ch_client, module, collection, cluster):
    query = f"DROP NAMED COLLECTION IF EXISTS {collection}"
    if cluster:
        query += (f" ON CLUSTER {cluster}")
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Named collection '{collection}' deleted"}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "collection": {"type": "str", "required": True, "aliases": ["name"]},
        "check": {"type":"bool", "default": False},
        "state": {"type": "str", "default": "present", "choices": ["present", "abscent"]},
        "cluster": {"type": "str", "required": False},
        "pg_user": {"type": "str", "required": False},
        "pg_pswd": {"type": "str", "required": False},
        "pg_host": {"type": "str", "required": False},
        "pg_port": {"type": "int", "default": 5432},
        "pg_db": {"type": "str", "required": False},
        "pg_schema": {"type": "str", "required": False}
    }

    result = {
        "changed": False
    }

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if module.check_mode:
        module.exit_json(**result)

    login_user = module.params["login_user"]
    login_password = module.params["login_password"]
    port = module.params["port"]
    host = module.params["host"]
    collection = module.params["collection"]
    check = module.params["check"]
    state = module.params["state"]
    cluster = module.params["cluster"]
    pg_user = module.params["pg_user"]
    pg_pswd = module.params["pg_pswd"]
    pg_host = module.params["pg_host"]
    pg_port = module.params["pg_port"]
    pg_db = module.params["pg_db"]
    pg_schema = module.params["pg_schema"]

    try:
        ch_client = get_client(username=login_user, password=login_password, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))

    if check:
        module.exit_json(**is_collection_exist(ch_client, collection))

    if state == 'present':
        result = create_collection(ch_client, module, collection, cluster, pg_user, pg_pswd, pg_host, pg_port, pg_db, pg_schema)
    elif state == 'abscent':
        result = drop_collection(ch_client, module, collection, cluster)
    else:
        raise Exception(f"Named collection state '{state}' unknown!")

    module.exit_json(**result)


if __name__ == '__main__':
    main()
