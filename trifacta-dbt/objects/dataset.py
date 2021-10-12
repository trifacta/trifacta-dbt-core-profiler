from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSeedNode
from trifacta.util.tfrequests import TrifactaEndpoint


def is_supported_dataset(node):
    if isinstance(node, ParsedModelNode):
        if node.is_ephemeral:
            return False
        return node.get_materialization() == "table" or node.get_materialization() == "view"
    elif isinstance(node, ParsedSeedNode):
        return True
    return False


def mk_import_dataset_request(node, name, conn_id, tfc, dbt):
    jdbc_path = get_jdbc_path(tfc, dbt);
    if isinstance(node, ParsedModelNode):
        if node.is_ephemeral:
            return None
        jdbc_type = None
        if node.get_materialization() == 'table':
            jdbc_type = "TABLE"
        elif node.get_materialization() == 'view':
            jdbc_type = "VIEW"
        else:
            return None
        req = {
            "name": name,
            "connectionId": conn_id,
            "type": "jdbc",
            "jdbcType": jdbc_type,
            "jdbcPath": jdbc_path,
            "jdbcTable": node.alias
        }
        return TrifactaEndpoint('POST', '/v4/importedDatasets', req)
    elif isinstance(node, ParsedSeedNode):
        req = {
            "name": name,
            "connectionId": conn_id,
            "type": "jdbc",
            "jdbcType": "TABLE",
            "jdbcPath": jdbc_path,
            "jdbcTable": node.alias
        }
        return TrifactaEndpoint('POST', '/v4/importedDatasets', req)
    else:
        return None


def get_jdbc_path(tfc, dbt):
    type = tfc["type"]
    if type == "dataprep":
        project = dbt.config.credentials.database
        dataset = dbt.config.credentials.schema
        return [project, dataset]
    else:
        schema = dbt.config.credentials.schema
        return [schema]


def mk_add_dataset_to_flow_request(ds_id, flow_id):
    req = {
        "flow": {
            "id": flow_id
        }
    }
    return TrifactaEndpoint('POST', f'/v4/importedDatasets/{ds_id}/addToFlow', req)


def mk_create_wrangled_dataset_request(name, ds_id, flow_id):
    req = {
        "importedDataset": {
            "id": ds_id
        },
        "inferredScript": {},
        "flow": {
            "id": flow_id
        },
        "name": name
    }
    return TrifactaEndpoint('POST', '/v4/wrangledDatasets', req)


def mk_create_output_object_request(w_ds_id, execution):
    req = {
        "execution": execution,
        "profiler": True,
        "isAdhoc": True,
        "flowNodeId": w_ds_id,
    }
    return TrifactaEndpoint('POST', '/v4/outputObjects', req)


def mk_create_write_setting_request(path, oobj_id):
    req = {
        "path": path,
        "action": "create",
        "format": "csv",
        "compression": "none",
        "header": True,
        "asSingleFile": True,
        "outputObjectId": oobj_id
    }
    return TrifactaEndpoint('POST', '/v4/writeSettings', req)


def mk_create_publication_request(oobj_id, output_table_name, connection_id, tfc, dbt):
    path = get_jdbc_path(tfc, dbt)
    req = {
        "path": path,
        "tableName": output_table_name,
        "targetType": "bigquery",
        "action": "createAndLoad",
        "connectionId": connection_id,
        "outputObjectId": oobj_id
    }
    return TrifactaEndpoint('POST', '/v4/publications', req)


def mk_create_sql_script(oobj_id, sql, type, vendor, connection_id):
    req = {
        "sqlScript": sql,
        "type": type,
        "vendor": vendor,
        "outputObjectId": oobj_id,
        "connection_id": connection_id
    }
    return TrifactaEndpoint('POST', '/v4/sqlScripts', req)
