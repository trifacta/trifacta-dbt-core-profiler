from task.base import BaseTask


class CleanTask(BaseTask):
    def __init__(self, args):
        super().__init__(args)

    @classmethod
    def pre_init_hook(cls, args):
        super().pre_init_hook(args)

    def run(self):
        pass
