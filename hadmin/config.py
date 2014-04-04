from xml.etree import ElementTree as ET
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class Config:
    """Holds a bunch of ConfigValues and processes them
    
    Can print them out for debugging, check for valid values, return
    an XML or Hadmin representation.

    """
    def __init__(self, config_value_array):
        if config_value_array == None:
            self.configs = dict(dict())
        else:
            self.configs = config_value_array

    def __str__(self):
        out = ""
        for key in self.configs:
            tmp = ""
            if self.configs[key]['final'] == "true":
                tmp = "final "
            tmp = tmp + key + ": " + self.configs[key]['value'] + "\n"
            out = out + tmp
        out = out[0:-2]
        return out

    @classmethod
    def from_xml(cls, filename):
        """Parse Hadoop XML and fill ConfigValues"""

        configs = dict()
        key = ""
        val = ""
        fnl = "false"

        for event, elem in ET.iterparse(filename):
            if elem.tag == "name":
                if key != "":
                    configs[key]['value'] = val
                    configs[key]['final'] = fnl
                    fnl = "false"
                key = elem.text
            elif elem.tag == "value":
                val = elem.text
            elif elem.tag == "final":
                fnl = elem.text
                configs[key]['value'] = val
                configs[key]['final'] = fnl
        return cls(configs)

    @classmethod
    def from_yaml(cls, filename):
        """Parse Hadmin's config and fill ConfigValues"""
        configs = dict()
        f = open(filename, "r")
        data = load(f, Loader=Loader)
        key = ""
        val = ""
        fnl = ""

        for elem in data:
            key = elem
            try:
                val = str(data[elem]['val'])
                fnl = data[elem]['final']
            except TypeError:
                val = str(data[elem])
                fnl = "false"

            configs[key] = dict()
            configs[key]['value'] = val
            configs[key]['final'] = fnl
        return cls(configs)

    def to_xml(self):
        """Return an XML representation of this Config"""
        out = ["<configuration>"]
        for key in self.configs:
            out.append("\t<property>");
            out.append("\t\t<name>" + key + "</name>")
            out.append("\t\t<value>" + self.configs[key]['value'] + "</value>")
            out.append("\t\t<final>" + self.configs[key]['final'] + "</final>")
            out.append("\t</property>")
        out.append("</configuration>")
        return '\n'.join(out)

# Execute a small demo if run as a script
if __name__ == "__main__":
    import sys

    if (len(sys.argv) < 2):
        print("Usage: " + __file__ + " <file>")
        exit(1)

    fname = sys.argv[1]
    if fname.split('.')[-1] == "xml":
        conf = Config.from_xml(fname)
    else:
        conf = Config.from_yaml(fname)

    print(conf)
    print(conf.to_xml())
