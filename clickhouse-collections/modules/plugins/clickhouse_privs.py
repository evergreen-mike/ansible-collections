#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_privs
short_description: назначение прав и ролей в clickhouse
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
    role:
        description:
            роль или пользователь, которому будут назначаться привилегии или роли.
            Может задаваться несколько пользователей и ролей через запятую.
        requeried: true
        aliases: [user]
        type: list
    grant_to:
        description:
            пользователь(и), которому будут назначены роли, указанные в 'role'.
            Обязательно должен быть указан либо этот параметр, либо параметр 'privs'.
        requeried: false
        type: list
    privs:
        description:
            привилегии, задаваемые пользователям или ролям. Обязательно должен быть указан
            либо этот параметр, либо параметр 'grant_to'. Значения задаются в виде словаря,
            где ключи имеют вид - 'db_name.table_name', а значения - привилегии, которые будут выданы.
            Могут быть указаны следующие привилегии: SELECT, INSERT, ALTER, CREATE, DROP, SHOW, TRUNCATE,
            OPTIMIZE, KILL QUERY, ACCESS MANAGMENT, SYSTEM, INTROSPECTION, SOURCES
        requeried: false
        type: dict
    state:
        description:
            если состояние установлено 'present'(по умолчанию), то указанные права будут выданы,
            если установлено 'abscent', то указанные права будут изъяты.
        default: present
        choices: [abscent, present]
        type: str
    cluser:
        description:
            название кластера clickhouse, на котором будут выполнены операции. Если не указан,
            то операции будут выполнены только на целевой ноде, указанной при запуске ansible-playbook.
        requeried: false
        type: str
    replace:
        description:
            заменяет все старые привилегии новыми привилегиями для пользователя или роли, если установлено в true.
            По умолчанию установлено в false, поэтому добавляет новые привилегии к уже существующим.
        default: false
        type: bool
    grant:
        description:
            разрешает пользователю или роли выполнять запрос GRANT. Пользователь может выдавать
            только те привилегии, которые есть у него, той же или меньшей области действий.
            Используется только совместно с параметром 'privs' при создани привилегий, но не при удалении.
        default: false
        type: bool
    admin:
        description:
            разрешает пользователю назначать свои роли другому пользователю.
            Используется только совместно с параметром 'grant_to' при назначении или отзыве ролей.
        default: false
        type: bool
'''

EXAMPLES = r'''
- name: Выдать привилегии ролям test_role, test_role_1 и пользователю user1 с заменой существующих привилегий
    clickhouse_privs:
      role: test_role, test_role_1, user1
      privs:
        'test_db.*': 'SELECT,UPDATE,ALTER,ACCESS MANAGEMENT'
        '*.*': 'SHOW USERS'
      cluster: my_cluster
      state: present
      replace: true

- name: назначение ролей test_role, test_role_1 пользователю user1, с правом назанчать эти роли другим пользователям и правом на выполнение команд grant
    clickhouse_privs:
      role:
        - test_role
        - test_role_1
      grant_to: user1
      admin: true
      grant: true
      cluster: my_cluster
      state: present

- name: отзыв роли test_role у пользователя user1
    clickhouse_privs:
      role: test_role
      grant_to: user1
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


def grant_privs(ch_client, module, role, privs, cluster, replace, grant):
    #result_queries = []
    for n,priv in enumerate(privs.items()):
        query_fragments = ["GRANT"]
        if cluster:
            query_fragments.append(f"ON CLUSTER {cluster}")
        query_fragments.append(f"{priv[1]} ON {priv[0]} TO {','.join(role)}")
        if grant:
            query_fragments.append(f"WITH GRANT OPTION")
        if replace and n == 0:                                  # replace нужен только на первом запросе
            query_fragments.append(f"WITH REPLACE OPTION")
        query = ' '.join(query_fragments)
        #result_queries.append(query)
        try:
            ch_client.command(query)
        except  Exception as e:
            return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    #raise Exception(result_queries)
    return {"changed": True, "msg": f"Privileges for {role} granted"}


def grant_role(ch_client, module, role, grant_to, cluster, replace, admin):
    query_fragments = ["GRANT"]
    if cluster:
        query_fragments.append(f" ON CLUSTER {cluster}")
    query_fragments.append(f" {','.join(role)} TO {','.join(grant_to)}")
    if admin:
        query_fragments.append("WITH ADMIN OPTION")
    if replace:
        query_fragments.append("WITH REPLACE OPTION")
    query = ' '.join(query_fragments)
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Granted {role} to {grant_to}"}


def revoke_privs(ch_client, module, role, privs, cluster):
    for objs, priv in privs.items():
        query_fragments = ["REVOKE"]
        if cluster:
            query_fragments.append(f"ON CLUSTER {cluster}")
        query_fragments.append(f"{priv} ON {objs} FROM {','.join(role)}")
        query = ' '.join(query_fragments)
        try:
            ch_client.command(query)
        except  Exception as e:
            return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Revoked priveleges from {role}"}


def revoke_role(ch_client, module, role, grant_to, cluster, admin):
    query_fragments = ["REVOKE"]
    if admin:
        query_fragments.append("ADMIN OPTION FOR")
    query_fragments.append(f"{','.join(role)} from {','.join(grant_to)}")
    if cluster:
        query_fragments.append(f" ON CLUSTER {cluster}")
    query = ' '.join(query_fragments)
    try:
        ch_client.command(query)
    except  Exception as e:
        return module.fail_json(to_native({"changed": False, "msg": f"{e}: Error on query: {query}"}))
    return {"changed": True, "msg": f"Revoked {role} from {grant_to}"}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "role": {"type": "list", "required": True, "aliases": ["user"]},   # здесь подразумеваются как роли, так и обычные пользователи, можно комбинировать в одном списке
        "grant_to": {"type": "list", "required": False},    # здесь указываются пользователи, которым назначаются роли
        "privs": {"type": "dict", "required": False},
        "state": {"type": "str", "default": "present", "choices": ["present", "abscent"]},
        "cluster": {"type": "str", "required": False},
        "replace": {"type": "bool", "default": False},
        "grant": {"type": "bool", "default": False}, # используется только при назначении привилегий
        "admin": {"type": "bool", "default": False}  # используется при назначении и отборе ролей
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
    role = module.params["role"]
    grant_to = module.params["grant_to"]
    privs = module.params["privs"]
    state = module.params["state"]
    cluster = module.params["cluster"]
    replace = module.params["replace"]
    grant = module.params["grant"]
    admin = module.params["admin"]


    try:
        ch_client = get_client(username=login_user, password=login_password, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))


    if state == 'present':
        if privs:
            result = grant_privs(ch_client, module, role, privs, cluster, replace, grant)
        elif grant_to:
            result = grant_role(ch_client, module, role, grant_to, cluster, replace, admin)
        else:
            raise Exception("'privs' or 'grant_to' parameter needs.")
    else:
        if privs:
            result = revoke_privs(ch_client, module, role, privs, cluster)
        elif grant_to:
            result = revoke_role(ch_client, module, role, grant_to, cluster, admin)
        else:
            raise Exception("'privs' or 'grant_to' parameter needs.")

    module.exit_json(**result)


if __name__ == '__main__':
    main()
