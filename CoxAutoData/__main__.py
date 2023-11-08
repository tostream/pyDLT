
import argparse
from .DeltaLake.Delta.executor import executor


def main():
    parser = argparse.ArgumentParser(description='Cox Delta Lake:')
    parser.add_argument('-t', '--tables', type=str,
                        help='name of table list declared in CoxFlowDLT.flow')
    parser.add_argument('-p','--packages', action='append', help='Python packages of pipelines definition')
    args = parser.parse_args()
    arguments = args.__dict__
    executor(**arguments)


if __name__ == '__main__':
    main()
