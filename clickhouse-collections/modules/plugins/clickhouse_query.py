#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_query
short_description: Run CLickhouse queries
options:
    login_user:
        description: login name for clickhouse session on a remote host. If not set, the 'default' user will be used.
        required: false
        type: str
    login_password:
        description: login_user password for clickhouse session on a remote host. If not set, 'None' will be used.
        required: false
        type: str
    port:
        description: set clickhouse session port. If not set, will default to 8123.
        required: false
        type: int
    host:
        description: set clickhouse session host. If not set, 'localhost' will be used.
        required: false
        type: str
    db:
        description: database name where queries should be executed. If not set, 'default' will be used.
        required: false
        type: str
    query: query or list of queries.
        description:
        required: true
        type: str
    parameters: list of positional arguments for queries.
        description:
        required: false
        type: list

'''

EXAMPLES = r'''
- name: execute_query_1
  clickhouse_query:
    db: test_db
    query: |
      INSERT INTO t1 VALUES
      (11,'Jhon'),(12,'Bill'),(13,'Jack'),(14,'Mike'),(15,'Rob')

- name: execute_query_2
  clickhouse_query:
    query: "CREATE USER IF NOT EXISTS test_user IDENTIFIED WITH sha256_password BY '1111'"
    db: test_db
    login_user: admin
    login_password: qwerty
    port: 8123

- name: select_query
  clickhouse_query:
    query: "select name from system.users where name != 'default'"
  register:
    res_query

- name: show var select query results
  debug:
    var: res_query.query_result
'''

RETURN = r'''
changed:
    description: status.
    type: str
    returned: always
executed_queries:
    description: list of queries were executed.
    type: list
    returned: always
'''

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native
from clickhouse_connect import get_client


def exec_query(ch_client, query, query_params):
    try:
        if query_params is not None:
            query = query % tuple(query_params)
        if query.split(' ')[0].upper() == "SELECT":
            result = ch_client.query(query)
            #raise Exception(result)
            return {"changed": False, "query_result": result.result_rows}
        ch_client.command(query)
    except:
        raise Exception(f'QueryError - {query}')
    return {"changed": True, "executed_query": query}


def main():

    module_args = {
        "login_user": {"type": "str", "required": False},
        "login_password": {"type": "str", "required": False},
        "port": {"type": "int", "required": False},
        "host": {"type": "str", "required": False},
        "db": {"type": "str", "required": False},
        "query": {"type": "str", "required": True},
        "parameters": {"type": "list", "required": False}
    }

    result = {
        "changed": False,
        "executed_queries": {"type": "list"}
    }

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if module.check_mode:
        module.exit_json(**result)

    login_user = module.params["login_user"]
    login_password = module.params["login_password"]
    db = module.params["db"]
    port = module.params["port"]
    host = module.params["host"]
    query = module.params["query"]
    parameters = module.params["parameters"]

    try:
        ch_client = get_client(username=login_user, password=login_password, database=db, port=port, host=host)
    except Exception as e:
        return module.fail_json(to_native(e))

    result = exec_query(ch_client, query, parameters)
    #raise Exception(result)
    module.exit_json(**result)


if __name__ == '__main__':
    main()
