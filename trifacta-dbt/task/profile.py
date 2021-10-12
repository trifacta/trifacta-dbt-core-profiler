import sys
import time

from task.base import BaseTask
from objects.connection import (
    mk_create_connection_request,
    mk_list_connections_request
)
from objects.dataset import (
    is_supported_dataset,
    mk_import_dataset_request,
    mk_add_dataset_to_flow_request,
    mk_create_wrangled_dataset_request,
    mk_create_output_object_request,
    mk_create_write_setting_request,
    mk_create_publication_request,
    mk_create_sql_script
)
from objects.flow import mk_create_flow_request
from objects.job import (
    mk_run_job_group_request,
    mk_get_job_status_request,
)


class ProfileTask(BaseTask):
    def __init__(self, args):
        super().__init__(args)

    def get_trifacta_prefix(self):
        return self.args.trifacta_prefix

    @classmethod
    def pre_init_hook(cls, args):
        super().pre_init_hook(args)

    def run(self):
        trifacta_prefix = self.get_trifacta_prefix()
        tfc = self.trifacta_config()
        tfr = self.trifacta()
        dbt = self.dbt()

        dataset_count = count_importable_datasets(self.args.include_list, dbt)
        c = single_yes_or_no_question(f"{dataset_count} datasets will be created and profiled. Continue?")
        if not c:
            return

        conn_id = self.connection_id(trifacta_prefix, tfr, dbt)
        print("Creating Flow: ", end="")
        sys.stdout.flush()
        flow_res = create_flow(trifacta_prefix, tfc, tfr, dbt)
        print("SUCCESS")
        flow_id = flow_res['id']
        fs_root = self.trifacta_config()["filesystem_root"]
        execution = get_execution(tfc)
        ds_res = import_datasets(trifacta_prefix, self.args.include_list, tfc, tfr, dbt, conn_id, flow_id, fs_root, execution)
        ds_jobs = []
        for ds in ds_res:
            i_ds = ds[0]
            w_ds = ds[1]
            w_ds_id = w_ds['id']
            print("Starting Profile Run for " + i_ds['name'] + ": ", end="")
            sys.stdout.flush()
            run_ep = mk_run_job_group_request(w_ds_id)
            run_res = run_ep.invoke(tfr).json()
            ds_jobs.append([i_ds['name'], run_res['id'], run_res['jobGraph']['vertices']])
            print("SUCCESS")
        spinner = spinning_cursor()
        for job in ds_jobs:
            ds_name = job[0]
            jobgroup_id = job[1]
            ds_vertices = job[2]
            print("Waiting for " + ds_name + " to complete:", end="")
            sys.stdout.flush()
            fail = False
            for v in ds_vertices:
                status_ep = mk_get_job_status_request(v)
                while True:
                    print(next(spinner), end="")
                    sys.stdout.flush()
                    status_res = status_ep.invoke(tfr).json()
                    if status_res == 'Cancelled' or status_res == 'Failed':
                        fail = True
                        sys.stdout.write('\b')
                        break
                    if status_res == 'Complete':
                        sys.stdout.write('\b')
                        break
                    time.sleep(0.5)
                    sys.stdout.write('\b')
            link_hostname = "cloud.trifacta.com"
            if tfc["type"] == "dataprep":
                link_hostname = "clouddataprep.com"
            if fail:
                print("FAILED")
            else:
                print(f"SUCCESS -> https://{link_hostname}/jobs/{jobgroup_id}?activeTab=profile")

    def connection_id(self, trifacta_prefix, tfr, dbt):
        tf_config = self.trifacta_config()
        conn_res = None
        configured_connection = tf_config.get("connection")
        if configured_connection:
            print(f"Looking up Connection ({configured_connection}): ", end="")
            sys.stdout.flush()
            conn_res = lookup_connection(tfr, configured_connection)
        else:
            print("Creating Connection: ", end="")
            sys.stdout.flush()
            conn_res = create_connection(trifacta_prefix, tfr, dbt)
        print("SUCCESS")
        conn_id = conn_res['id']
        return conn_id


def single_yes_or_no_question(question, default_no=True):
    choices = ' [y/N]: ' if default_no else ' [Y/n]: '
    default_answer = 'n' if default_no else 'y'
    reply = str(input(question + choices)).lower().strip() or default_answer
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return False if default_no else True


def spinning_cursor():
    while True:
        for cursor in '|/-\\':
            yield cursor


def create_name_prefix(trifacta_prefix, dbt):
    manifest = dbt.get_manifest()
    pid = manifest.metadata.project_id
    return '{}dbtprofile_{}'.format(trifacta_prefix, pid)


def create_connection(trifacta_prefix, tfr, dbt):
    name = '{}_connection'.format(create_name_prefix(trifacta_prefix, dbt))
    ep = mk_create_connection_request(dbt.config.credentials, name)
    return ep.invoke(tfr).json()


def lookup_connection(tfr, name):
    ep = mk_list_connections_request()
    conns = ep.invoke(tfr).json()
    print(conns)
    for conn in conns['data']:
        if conn['name'] == name:
            return conn
    raise Exception(f"Unable to find connection named {name}")


def count_importable_datasets(include_list, dbt):
    count = 0
    for node in dbt.get_manifest().nodes.values():
        if include_list and node.name not in include_list:
            continue
        if is_supported_dataset(node):
            count = count + 1
    return count


def import_datasets(trifacta_prefix, include_list, tfc, tfr, dbt, conn_id, flow_id, fs_root, execution):
    datasets = []
    name_prefix = create_name_prefix(trifacta_prefix, dbt)
    for node in dbt.get_manifest().nodes.values():
        if include_list and node.name not in include_list:
            print("Skipping {}".format(node.name))
            continue
        ds_name = '{}_{}_dataset'.format(name_prefix, node.name)
        ds_ep = mk_import_dataset_request(node, ds_name, conn_id, tfc, dbt)
        if ds_ep:
            print("Importing Dataset " + node.name + ": ", end="")
            sys.stdout.flush()
            ds_res = ds_ep.invoke(tfr).json()
            ds_id = ds_res['id']
            add_ep = mk_add_dataset_to_flow_request(ds_id, flow_id)
            add_res = add_ep.invoke(tfr).json()
            recipe_name = '{}_{}_recipe'.format(name_prefix, node.name)
            wrangle_ep = mk_create_wrangled_dataset_request(recipe_name, ds_id, flow_id)
            wrangle_res = wrangle_ep.invoke(tfr).json()
            w_ds_id = wrangle_res['id']
            oobj_ep = mk_create_output_object_request(w_ds_id, execution)
            oobj_res = oobj_ep.invoke(tfr).json()
            oobj_id = oobj_res['id']
            if tfc["type"] == "dataprep":
                output_table_name = "trifacta_dbt_temp_{}".format(oobj_id)
                pub_ep = mk_create_publication_request(oobj_id, output_table_name, conn_id, tfc, dbt)
                pub_ep.invoke(tfr)
                sql_ep = mk_create_sql_script(oobj_id, "DROP TABLE `{}`.`{}`".format(dbt.config.credentials.schema, output_table_name), "post", "bigquery", conn_id)
                sql_ep.invoke(tfr)
            else:
                out_path = '{}/{}_{}_out.csv'.format(fs_root, name_prefix, node.name)
                ws_ep = mk_create_write_setting_request(out_path, oobj_id)
                ws_ep.invoke(tfr)
            datasets.append((ds_res, wrangle_res))
            print("SUCCESS")
    return datasets


def create_flow(trifacta_prefix, tfc, tfr, dbt):
    name = '{}_flow'.format(create_name_prefix(trifacta_prefix, dbt))
    ep = mk_create_flow_request(name, tfc)
    res = ep.invoke(tfr).json()
    return res


def get_execution(trifacta_config):
    type = trifacta_config["type"]
    if type == "dataprep":
        return "dataflow"
    else:
        return "emrSpark"
