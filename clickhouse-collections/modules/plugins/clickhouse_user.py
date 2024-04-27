#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module:
short_description:
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
            имя создаваемого пользователя
        required: false
        aliases: [user]
        type: str
    check:
        description:
            прверить, существует ли указанный пользователь на сервере clickhouse.
            Используется только с параметром name.
            Возвращает результат в __имя_переменной__.exists в виде булевого значения.
        default: false
        type: bool
    state:
        description:
            если состояние установлено 'present'(по умолчанию), то указанный пользователь будет создан,
            если установлено 'abscent', то указанный пользователь будет удалён.
        default: present
        choices: [abscent, present]
        type: str
    cluster:
        description:
            название кластера clickhouse, на котором будут выполнены операции. Если не указан,
            то операции будут выполнены только на целевой ноде, указанной при запуске ansible-playbook.
        requeried: false
        type: str
    auth_type:
        description:
            задать тип аутентификации - без пароля, по паролю или по хэшу
        default: sha256_password
        choices: [no_password, plaintext_password, sha256_password, sha256_hash, double_sha1_password, double_sha1_hash]
    auth:
        description:
            пароль или хэш для прохождения аутентификации заданного в auth_type типа
        required: False
        aliases: [pswd, hash]
        type: str
    allowed_hosts:
        description:
            хосты или подсети (с указанием маски), с которых пользователю разрешено устанавливать соединения с сервером clickhouse.
            Если параметр не задан, то пользователю разрешено подключаться с любых хостов.
        required: False
        type: list
    roles:
        description:
            задать для пользователя роли по умолчанию
        default: NONE
        type: list
    database:
        description:
            база данных, к которой пользователь подключается по умолчанию
        default: NONE
        aliases: [db, default_db]
        type: str
    grantees:
        description:
            пользователи или роли, которым разрешено получать привилегии от создаваемого
            пользователя при условии, что этому пользователю также предоставлен весь
            необходимый доступ с использованием GRANT OPTION
        default: NONE
        type: list
    settings:
        description:
            задать настройки базы данных для конкретного пользователя, допустимые значения соответсвуют
            параметрам настроек базы данных clickhouse
        requeried: false
        type: dict
'''

EXAMPLES = r'''
- name: создать пользователя user1, которому по дефолту будут назначены все роли и который может устанавливать права любым другим пользователям
    clickhouse_user:
      login_user: admin
      login_password: qwerty
      name: user1
      grantees: ANY
      roles: ALL
      cluster: my_cluster
      settings:
        readonly: 1
        insert_allow_materialized_columns: 0

- name: создать пользователя user2, с правами выдавать привилегии пользователям user1 и user3, и имеющий дефотные роли test_role и test_role_1
    clickhouse_user:
      name: user2
      state: present
      grantees:
        - user1
        - user3
      role:
        - test_role
        - test_role_1
      cluster: my_cluster
      database: test_db

- name: удалить пользователя user1
   clickhouse_user:
     user: user1
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

def is_user_exists(ch_client, name):
    return {"exists": ch_client.command(f"SELECT count(*) FROM system.users WHERE name = '{name}'") > 0}

def create_user(ch_client, module, name, cluster, auth_type, auth, roles, database, grantees, settings):
    status = "created"
    query_fragments = [f"CREATE USER {name}"]
    if is_user_exists(ch_client, name)["exists"]:
        query_fragments = [f"ALTER USER {name}"]
        status = "changed"
    if cluster:
        query_fragments.append(f"ON CLUSTER {cluster}")
    if auth_type:
        query_fragments.append(f"IDENTIFIED WITH {auth_type}")
    if auth:
        query_fragments.append(f"BY '{auth}'")
    if roles:
        query_fragments.append(f"DEFAULT ROLE {','.join(roles)}")
    if database:
        query_fragments.append(f"DEFAULT DATABASE {database}")
    if grantees:
        query_fragments.append(f"GRANTEES {','.join(grantees)}")
    if settings:
        settings_kit = [f'{k}={v} READONLY' for k,v in settings.items()]
        query_fragments.append("SETTINGS " + ", ".join(settings_kit))
    query = ' '.join(query_fragments)
    #raise Exception(query)
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"User '{name}' {status}"}


def drop_user(ch_client, module, name, cluster):
    query = f"DROP USER IF EXISTS {name}"
    if cluster:
        query += f" ON CLUSTER {cluster}"
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"User '{name}' deleted"}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "name": {"type": "str", "required": True, "aliases": ["user"]},
        "check": {"type": "bool", "default": False},
        "state": {"type": "str", "default": "present", "choices": ["abscent", "present"]},
        "cluster": {"type": "str", "required": False},
        "auth_type": {"type": "str", "required": False, "choices": ["no_password", "plaintext_password", "sha256_password", "sha256_hash", "double_sha1_password", "double_sha1_hash"]},
        "auth": {"type": "str", "required": False, "aliases": ["pswd", "hash"]},
        "roles": {"type": "list", "required": False},
        "database": {"type": "str", "required": False, "aliases": ["db", "default_db"]},
        "grantees": {"type": "list", "required": False},
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
    auth_type = module.params["auth_type"]
    auth = module.params["auth"]
    roles = module.params["roles"]
    database = module.params["database"]
    grantees = module.params["grantees"]
    settings = module.params["settings"]

    try:
        ch_client = get_client(username=login_user, password=login_password, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))

    if check:
        module.exit_json(**is_user_exists(ch_client, name))

    if state =='present':
        result = create_user(ch_client, module, name, cluster, auth_type, auth, roles, database, grantees, settings)
    else:
        result = drop_user(ch_client, module, name, cluster)

    module.exit_json(**result)


if __name__ == '__main__':
    main()
