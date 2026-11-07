"""Offline, OS-authorized identity operations. Stop service before restore.

No secrets in argv/environment/stdout. No HTTP registration.
"""
import argparse
import getpass
import sys
from core import Error
from panel_identity import PanelIdentity


def secret(label):
    if not sys.stdin.isatty(): raise Error('密码必须通过TTY输入',400)
    value=getpass.getpass(label)
    if value!=getpass.getpass('再次输入：'): raise Error('两次密码不同',400)
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True)
    parser.add_argument('--control-directory',required=True)
    commands=parser.add_subparsers(dest='operation',required=True)
    init=commands.add_parser('init'); init.add_argument('login')
    restore=commands.add_parser('restore'); restore.add_argument('backup'); restore.add_argument('new_login')
    restore.add_argument('--service-stopped',action='store_true',required=True)
    reset=commands.add_parser('reset'); reset.add_argument('account_id')
    reset.add_argument('--role',choices=('admin','user'),required=True)
    reset.add_argument('--enabled',choices=('yes','no'),required=True)
    reset.add_argument('--root-user-id',type=int)
    args=parser.parse_args()
    try:
        pw=secret('新密码：')
        kernel=PanelIdentity(args.database,args.control_directory)
        if args.operation=='init': kernel.initialize(args.login,pw)
        elif args.operation=='restore': kernel.restore(args.backup,args.new_login,pw)
        else: kernel.offline_reset(args.account_id,pw,role=args.role,enabled=args.enabled=='yes',root_user_id=args.root_user_id)
        print('离线身份操作已持久完成。')
        return 0
    except (Error,OSError,ValueError):
        print('离线身份操作未完成；请核对受控状态。',file=sys.stderr)
        return 1


if __name__=='__main__': raise SystemExit(main())
