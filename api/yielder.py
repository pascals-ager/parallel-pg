import json


class LineYldr:

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
        with open(src_file, 'r') as f:
            for i in f:
                yield (
                    json.dumps(json.loads(self.format_suffix(self.format_prefix(i.strip('\n'), '['), ']').strip(',')))
                    ,)
