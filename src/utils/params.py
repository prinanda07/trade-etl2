from awsglue.utils import getResolvedOptions

from src.utils.logger_builder import LoggerBuilder


class Params(object):
    """Helper class which retrieves job input parameters"""

    def __init__(self, command_line_args):
        self.command_line_args = command_line_args
        self.log = LoggerBuilder().build()

    def get(self, name):
        """Get parameter from command line arguments"""
        options = getResolvedOptions(self.command_line_args, [name])
        self.log.info('{} = {}'.format(name, options[name]))
        return options[name]
