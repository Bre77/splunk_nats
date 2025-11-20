import sys
import import_declare_test

from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators
from nats_kv_command import generate

@Configuration()
class NatskvCommand(GeneratingCommand):
    """

    ##Syntax
    | natskv bucket=<bucket> key=<key> account=<account>

    ##Description
    Retrieve history from NATS JetStream Key-Value store

    """
    account = Option(name='account', require=True)
    bucket = Option(name='bucket', require=True)
    key = Option(name='key', require=True, default='>')

    def generate(self):
       return generate(self)

dispatch(NatskvCommand, sys.argv, sys.stdin, sys.stdout, __name__)