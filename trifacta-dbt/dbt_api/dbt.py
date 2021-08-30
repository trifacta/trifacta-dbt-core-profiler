from dbt.main import parse_args
from dbt.config.runtime import RuntimeConfig
from dbt.parser.manifest import ManifestLoader
from dbt.adapters.factory import register_adapter
from dbt.tracking import track_invocation_start, track_invocation_end, flush
from contextlib import contextmanager
from dbt.tracking import disable_tracking


class DBT:
    def __init__(self, dbt_args):
        disable_tracking()
        self.parsed = parse_args(args=dbt_args)
        self.config = RuntimeConfig.from_args(self.parsed)
        register_adapter(self.config)

    def get_manifest(self):
        with tracker(config=self.config, args=self.parsed):
            return ManifestLoader.get_full_manifest(config=self.config)


@contextmanager
def tracker(config, args):
    track_invocation_start(config, args)
    try:
        yield
        track_invocation_end(config, args, result_type="ok")
    finally:
        flush()
