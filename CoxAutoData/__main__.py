
import argparse
from .DeltaLake.Delta.executor import executor


def main():
    parser = argparse.ArgumentParser(description='Cox Delta Lake:')
    parser.add_argument('-t', '--tables', type=str,
                        help='name of table list declared in CoxFlowDLT.flow')
    parser.add_argument('-p','--packages', action='append', help='Python packages of pipelines definition')
    parser.add_argument('-f', '--flow', type=str, metavar='', required=False, nargs='?',
                    choices=['raw', 'ideal', 'bo'], default='raw', const='raw',
                    help='[raw | ideal | bo] : optional. The default value is raw.')
    parser.add_argument('-m', '--module', type=str, metavar='', required=False,
                    help='module name of table list. The default value is CoxFlowDLT.flow.')
    args = parser.parse_args()
    arguments = args.__dict__
    executor(**arguments)


if __name__ == '__main__':
    main()
