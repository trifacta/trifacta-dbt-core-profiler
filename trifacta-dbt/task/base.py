from dbt_api.dbt import DBT
from trifacta.util.tfconfig import load_config
from trifacta.util.tfrequests import TrifactaRequest


class NoneConfig:
    @classmethod
    def from_args(cls, args):
        return None


class BaseTask:
    def __init__(self, args):
        self.args = args
        self.tf_config = load_config(config_path=args.trifacta_config, profile=args.trifacta_profile)

    @classmethod
    def pre_init_hook(cls, args):
        pass

    @classmethod
    def from_args(cls, args):
        return cls(args)

    def dbt(self):
        dbt_args = [
            'list',
            '--profiles-dir',
            "{}".format(self.args.dbt_profiles_dir),
            '--project-dir',
            "{}".format(self.args.dbt_project_dir)
        ]
        if hasattr(self.args, "dbt_profile") and self.args.dbt_profile:
            dbt_args.extend([
                "--profile",
                "{}".format(self.args.dbt_profile)
            ])
        if hasattr(self.args, "dbt_target") and self.args.dbt_target:
            dbt_args.extend([
                "--target",
                "{}".format(self.args.dbt_target)
            ])
        return DBT(dbt_args)

    def trifacta_config(self):
        return self.tf_config

    def trifacta(self):
        tfr = TrifactaRequest(self.tf_config)
        return tfr

    def run(self):
        raise NotImplemented()

