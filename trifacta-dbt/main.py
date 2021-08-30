import sys
import logging
import argparse
from dbt.config import PROFILES_DIR as DBT_PROFILES_DIR
from trifacta.util.tfconfig import TRIFACTA_CONFIG_PATH
import os
from _version import __version__
from task.diagnostics import DiagnosticsTask
from task.profile import ProfileTask
from task.clean import CleanTask


def main(args=None):
    if args is not None:
        args = sys.argv[1:]
    execute(args)


def execute(args):
    print(args)
    parsed = parse_args(args)
    parsed.cls.pre_init_hook(parsed)

    task = parsed.cls.from_args(args=parsed)
    task.run()


def _build_base_parser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--dbt-project-dir',
        default=".",
        type=str,
        help='''
        Directory that holds the DBT project (dbt_project.yml).
        '''
    )

    base_subparser.add_argument(
        '--dbt-profiles-dir',
        default=DBT_PROFILES_DIR,
        type=str,
        help='''
        Directory that contains profiles.yml
        '''
    )

    base_subparser.add_argument(
        '--dbt-profile',
        required=False,
        type=str,
        help='''
        Profile to load. Overrides setting in dbt_project.yml
        '''
    )

    base_subparser.add_argument(
        '--dbt-target',
        default=None,
        type=str,
        help='''
        Target to load for the given profile
        ''',
    )

    base_subparser.add_argument(
        '--trifacta-config',
        default=TRIFACTA_CONFIG_PATH,
        type=str,
        help='''
        Directory that contains trifacta config
        '''
    )

    base_subparser.add_argument(
        '--trifacta-profile',
        default='DEFAULT',
        type=str,
        help='''
        Profile in trifacta config to use
        ''',
    )

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser


def _build_diagnostics_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'diagnostics',
        parents=[base_subparser],
        help='''
        Show diagnostic info
        ''',
        aliases=['diag']
    )

    sub.set_defaults(cls=DiagnosticsTask)

    return sub


def _build_profile_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'profile',
        parents=[base_subparser],
        help='''
        Install Profiling flow in Trifacta
        '''
    )

    sub.add_argument(
        '--trifacta-prefix',
        default="",
        type=str,
        help='''
        Prefix to use for artifacts created in Trifacta
        '''
    )

    sub.add_argument(
        '--include-list',
        default=None,
        type=str,
        help='''
        Comma separated list of tables to include in profiling
        '''
    )

    sub.set_defaults(cls=ProfileTask)

    return sub


def _build_clean_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'clean',
        parents=[base_subparser],
        help='''
        Delete Profiling flow in Trifacta
        '''
    )

    sub.add_argument(
        '--trifacta-prefix',
        default="",
        type=str,
        help='''
        Prefix to use for artifacts created in Trifacta
        '''
    )

    sub.set_defaults(cls=CleanTask)

    return sub


def parse_args(args):
    p = argparse.ArgumentParser(
        prog='trifacta-dbt',
        description='''
        Trifacta DBT Integration
        ''',
    )

    p.add_argument(
        '--version',
        action='version',
        version='%(prog)s {version}'.format(version=__version__),
        help='''
        Show version
        '''
    )

    subs = p.add_subparsers(title='Available sub-commands')

    base_parser = _build_base_parser()

    _build_diagnostics_subparser(subs, base_parser)
    _build_profile_subparser(subs, base_parser)
    _build_clean_subparser(subs, base_parser)

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if hasattr(parsed, 'dbt_profiles_dir'):
        parsed.dbt_profiles_dir = os.path.abspath(parsed.dbt_profiles_dir)

    if getattr(parsed, 'dbt_project_dir', None) is not None:
        expanded_user = os.path.expanduser(parsed.dbt_project_dir)
        parsed.dbt_project_dir = os.path.abspath(expanded_user)

    if hasattr(parsed, 'trifacta_config'):
        parsed.trifacta_config = os.path.abspath(parsed.trifacta_config)

    if hasattr(parsed, 'include_list') and parsed.include_list:
        parsed.include_list = parsed.include_list.split(',')

    return parsed


if __name__ == "__main__":
    main(sys.argv)
