from trifacta.util.tfrequests import TrifactaEndpoint


def mk_run_job_group_request(w_ds_id):
    req = {
        "wrangledDataset": {
            "id": w_ds_id
        }
    }
    return TrifactaEndpoint('POST', '/v4/jobGroups', req)


def mk_get_job_status_request(job_id):
    return TrifactaEndpoint('GET', f'/v4/jobs/{job_id}/status')
