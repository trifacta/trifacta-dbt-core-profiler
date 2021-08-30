from task.base import BaseTask


class DiagnosticsTask(BaseTask):
    def __init__(self, args):
        super().__init__(args)

    @classmethod
    def pre_init_hook(cls, args):
        super().pre_init_hook(args)

    def run(self):
        dbt = self.dbt()
        manifest = dbt.get_manifest()
        print(manifest)
        print('DBT Manifest: OK')

        tc = self.trifacta()
        print('Trifacta connection: OK')

