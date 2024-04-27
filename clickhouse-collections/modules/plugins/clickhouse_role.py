#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_role
short_description: создание ролей в clickhouse
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
    name:
        description:
            имя создаваемой роли
        required: false
        aliases: [role]
        type: str
    check:
        description:
            прверить, существует ли указанная роль на сервере clickhouse.
            Используется только с параметром name.
            Возвращает результат в __имя_переменной__.exists в виде булевого значения.
        default: false
        type: bool
    state:
        description:
            если состояние установлено 'present'(по умолчанию), то указанная роль будет создана,
            если установлено 'abscent', то указанная роль будет удалена.
        default: present
        choices: [abscent, present]
        type: str
    cluster:
        description:
            название кластера clickhouse, на котором будут выполнены операции. Если не указан,
            то операции будут выполнены только на целевой ноде, указанной при запуске ansible-playbook.
        requeried: false
        type: str
    settings:
        description:
            задать настройки базы данных для конкретной роли, допустимые значения соответсвуют
            параметрам настроек базы данных clickhouse
        requeried: false
        type: dict
'''

EXAMPLES = r'''
- name: создать роль test_role под пользователем admin
    clickhouse_role:
      login_user: admin
      login_password: qwerty
      name: test_role
      cluster: my_cluster
      settings:
        readonly: 1
        insert_allow_materialized_columns: 0

- name: создать роль test_role_1 под дефолтным пользователем clickhouse
    clickhouse_role:
      name: test_role_1
      cluster: my_cluster
      state: present

- name: удалить роль test_role
    clickhouse_role:
      name: test_role
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

def is_role_exists(ch_client, name):
    return {"exists": ch_client.command(f"SELECT count(*) FROM system.roles WHERE name = '{name}'") > 0}

def create_role(ch_client, module, name, cluster, settings):
    query_fragments = [f"CREATE ROLE IF NOT EXISTS {name}"]
    if cluster:
        query_fragments.append(f"ON CLUSTER {cluster}")
    if settings:
        settings_kit = [f'{k}={v} READONLY' for k,v in settings.items()]
        query_fragments.append("SETTINGS " + ", ".join(settings_kit))
    query = ' '.join(query_fragments)
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Role '{name}' created or changed"}


def drop_role(ch_client, module, name, cluster):
    query = f"DROP ROLE IF EXISTS {name}"
    if cluster:
        query += f" ON CLUSTER {cluster}"
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Role '{name}' deleted"}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "name": {"type": "str", "required": True, "aliases": ["role"]},
        "check": {"type": "bool", "default": "false"},
        "state": {"type": "str", "default": "present", "choices": ["abscent", "present"]},
        "cluster": {"type": "str", "required": False},
        "settings": {"type": "dict", "required": False}
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
    name = module.params["name"]
    check = module.params["check"]
    state = module.params["state"]
    cluster = module.params["cluster"]
    settings = module.params["settings"]

    try:
        ch_client = get_client(username=login_user, password=login_password, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))

    if check:
        module.exit_json(**is_role_exists(ch_client, name))

    if state == 'present':
        result = create_role(ch_client, module, name, cluster, settings)
    else:
        result = drop_role(ch_client, module, name, cluster)

    module.exit_json(**result)


if __name__ == '__main__':
    main()
