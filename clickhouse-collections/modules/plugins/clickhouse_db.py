#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_db
short_description: создание и удаление баз данных в clickhouse
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
    db_name:
        description:
            имя создаваемой или удаляемой базы данных
        required: true
        aliases: [db, database]
        type: str
    state:
        description:
            если состояние установлено 'present'(по умолчанию), то указанная база данных будет создана,
            если установлено 'abscent', то указанная база данных будет удалена.
        default: present
        choices: [abscent, present]
        type: str
    cluster:
        description:
            название кластера clickhouse, на котором будут выполнены операции. Если не указан,
            то операции будут выполнены только на целевой ноде, указанной при запуске ansible-playbook.
        requeried: false
        type: str
    engine:
        description:
            движок, используемый при создании базы данных. Если не указан, то по умолчанию clickhouse
            использует движок собственный движок баз данных 'Atomic'.
        requeried: false
        type: str
    engine_settings:
        description:
            параметры подключения, при использовании движка базы данных для внешних источников
        requeried: false
        type: dict
'''

EXAMPLES = r'''
- name: создание базы данных test_db
    clickhouse_db:
      db_name: test_db
      engine: Atomic
      cluster: my_cluster

  - name: создать базу данных test_2
    clickhouse_db:
      db_name: test_2
      engine: PostgreSQL
      engine_settings:
        'host:port': '158.160.17.193:5432'
        'database': 'pg_test_db'
        'user': 'pgadmin'
        'password': '4r5t6y7u'
      cluster: my_cluster

- name: удаление базы данных test_db
   clickhouse_db:
     db_name: test_db
     state: abscent
     cluster: my_cluster
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


def is_db_exist(ch_client, db_name):
    return ch_client.command(f"SELECT count(*) FROM system.databases WHERE name = '{db_name}'") > 0


def create_db(ch_client, db_name, cluster, engine, engine_settings):
    if is_db_exist(ch_client, db_name):
        return {"changed": False, "msg": f"Database '{db_name}' alredy exists"}
    query_fragments = [f"CREATE DATABASE {db_name}"]
    if cluster:
        query_fragments.append(f"ON CLUSTER {cluster}")
    if engine:
        query_fragments.append(f"ENGINE = {engine}")
        if engine_settings:
            settings = [ f"'{setting}'" for setting in engine_settings.values() ]
            query_fragments.append(f"({', '.join(settings)})")
    query = ' '.join(query_fragments)
    # raise Exception(query)
    ch_client.command(query)
    return {"changed": True, "msg": f"Database '{db_name}' created"}


def drop_db(ch_client, db_name, cluster):
    query = f"DROP DATABASE IF EXISTS {db_name}"
    if cluster:
        query += f" ON CLUSTER {cluster}"
    ch_client.command(query)
    return {"changed": True, "msg": f"Database '{db_name}' deleted"}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "db_name": {"type": "str", "required": True, "aliases": ["db", "database"]},
        "state": {"type": "str",  "default": "present", "choices": ["abscent", "present"]},
        "cluster": {"type": "str", "required": False},
        "engine": {"type": "str", "required": False},
        "engine_settings": {"type": "dict", "required": False}
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
    db_name = module.params["db_name"]
    state = module.params["state"]
    cluster = module.params["cluster"]
    engine = module.params["engine"]
    engine_settings = module.params["engine_settings"]

    try:
        ch_client = get_client(username=login_user, password=login_password, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))


    if state == 'present':
        result = create_db(ch_client, db_name, cluster, engine, engine_settings)
    else:
        result = drop_db(ch_client, db_name, cluster)

    module.exit_json(**result)


if __name__ == '__main__':
    main()
