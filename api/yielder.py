import json
import logging


class LineYldr:

    def __init__(self):
        self.logger = logging.getLogger()

    @staticmethod
    def format_prefix(line, prefix):
        """
        :param line: line to format
        :param prefix: prefix to remove
        :return: formatted line after removing prefix
        """
        if line.startswith(prefix):
            return line[len(prefix):]
        return line

    @staticmethod
    def format_suffix(line, suffix):
        """
        :param line: line to format
        :param suffix: suffix to strip
        :return: formatted line after stripping suffix
        """
        if line.endswith(suffix):
            return line[:-len(suffix)]
        return line

    def yield_line(self, src_file):
        """
        :param src_file: src file to read
        :yield: tuple formatted line
        """
        with open(src_file, 'r') as file:
            for line in file:
                try:
                    json_dict = json.loads(self.format_suffix(self.format_prefix(line.strip('\n'), '['), ']').strip(','))
                    """
                    if the schema of the json is exchanged before hand, the schema validation logic can go here.
                    """
                    json_str = json.dumps(json_dict)
                    yield (json_str,)
                except json.JSONDecodeError as e:
                    self.logger.error("Json Decode error {}".format(e))
                    raise e
                except Exception as e:
                    self.logger.error("Exception occurred".format(e))
                    raise e


