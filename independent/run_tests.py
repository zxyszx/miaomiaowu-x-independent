#!/usr/bin/env python3
"""Run independent backend checks, explicitly selecting asyncio debug mode."""
import argparse
import asyncio
from pathlib import Path
import unittest


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-mode',action='store_true',
                        help='disable asyncio debug tracing; keep all test assertions')
    parser.add_argument('tests',nargs='*',help='optional unittest test names')
    args=parser.parse_args()
    loader=unittest.defaultTestLoader
    suite=(loader.loadTestsFromNames(args.tests) if args.tests else
           loader.discover(str(Path(__file__).resolve().parent),pattern='test_*.py'))
    if args.runtime_mode:
        for test in cases(suite):
            if isinstance(test,unittest.IsolatedAsyncioTestCase):
                original_setup=test.asyncSetUp
                async def setup(original=original_setup):
                    asyncio.get_running_loop().set_debug(False)
                    await original()
                test.asyncSetUp=setup
    print('Asyncio mode: '+('runtime (debug tracing disabled)' if args.runtime_mode else
                           'debug (unittest default)'),flush=True)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    return not result.wasSuccessful()


if __name__=='__main__': raise SystemExit(main())
