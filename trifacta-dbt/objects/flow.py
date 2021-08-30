from trifacta.util.tfrequests import TrifactaEndpoint


def mk_create_flow_request(name, tfc):
    optimizers = None
    if tfc["type"] == "dataprep":
        optimizers = {
            "columnPruning": "enabled",
            "filterPushdown": "enabled",
            "columnPruningOnSource": "enabled",
            "filterPushdownOnSource": "enabled",
            "joinPushdown": "enabled",
            "expressionPushdown": "enabled",
            "aggPushdown": "enabled",
            "unionPushdown": "enabled",
            "profilePushdown": "enabled"
        }
    req = {
        "name": name,
        "settings": {
            "optimize": "enabled",
            "optimizers": optimizers
        }
    }
    return TrifactaEndpoint('POST', '/v4/flows', req)
