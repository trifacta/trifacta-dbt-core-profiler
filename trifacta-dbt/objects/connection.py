from dbt.adapters.snowflake import SnowflakeCredentials
from trifacta.util.tfrequests import TrifactaEndpoint


def mk_create_connection_request(credentials, name):
    type = credentials.type
    if type == 'snowflake':
        return TrifactaEndpoint('POST', '/v4/connections', _xlat_snowflake_connection(credentials, name))
    raise Exception("Unsupported type: " + type)


def mk_list_connections_request():
    return TrifactaEndpoint('GET', '/v4/connections')


def _xlat_snowflake_connection(credentials: SnowflakeCredentials, name):
    conn = {
        'vendor': 'snowflake',
        'vendorName': 'snowflake',
        'name': name,
        'type': 'jdbc',
        'host': credentials.account,
        'params': {
            'database': credentials.database,
            'schema': credentials.schema
        },
        'credentials': []
    }
    if credentials.password:
        conn['credentialType'] = 'basic'
        conn['credentials'].append({
            'username': credentials.user,
            'password': credentials.password
        })
    if credentials.warehouse:
        conn['params']['warehouse'] = credentials.warehouse
    if credentials.role:
        conn['params']['role'] = credentials.role

    return conn

