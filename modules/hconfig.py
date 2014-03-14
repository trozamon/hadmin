from xml.etree import ElementTree as ET
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class ConfigValue:
    """Keeps track of Hadoop config values

    Keeps values from reading either hadmin files or XML files.
    Used as a storage place with a little bit of error checking built
    in.

    """
    def __init__(self):
        self.is_final = False
        self.key = ""
        self.value = ""

    def __str__(self):
        ret = ""
        if self.is_final:
            ret += "final"
        ret += "\t" + self.key + "\t" + str(self.value)
        return ret

    def set_is_final(self, string):
        """Parses a string to set is_final as True or False"""
        if string == "true":
            self.is_final = True
        else:
            self.is_final = False

    def to_xml(self, num_tabs):
        """Outputs an XML representation"""
        out = [(num_tabs * "\t") + "<name>" + self.key + "</name>"]
        out.append((num_tabs * "\t") + "<value>" + str(self.value) + "</value>")
        if self.is_final:
            out.append((num_tabs * "\t") + "<final>true</final>")

        return "\n".join(out)

class Config:
    """Holds a bunch of ConfigValues and processes them
    
    Can print them out for debugging, check for valid values, return
    an XML or Hadmin representation.

    """
    def __init__(self, config_value_array):
        if config_value_array == None:
            self.configs = []
        else:
            self.configs = config_value_array

    def __str__(self):
        ret = ""
        for conf in self.configs:
            ret += conf.__str__() + "\n"
        ret = ret.rstrip("\n")
        return ret

    @classmethod
    def from_xml(cls, filename):
        """Parse Hadoop XML and fill ConfigValues"""
        configs = []
        tmp = ConfigValue()

        for event, elem in ET.iterparse(filename):
            if elem.tag == "name":
                if tmp.key != "":
                    configs.append(tmp)
                    tmp = ConfigValue()
                tmp.key = elem.text
            elif elem.tag == "value":
                tmp.value = elem.text
            elif elem.tag == "final":
                tmp.is_final = elem.text
                configs.append(tmp)
                tmp = ConfigValue()
        if len(tmp.key) > 0:
            configs.append(tmp)
        return cls(configs)

    @classmethod
    def from_yaml(cls, filename):
        """Parse Hadmin's config and fill ConfigValues"""
        configs = []
        tmp = ConfigValue()
        f = open(filename, "r")
        data = load(f, Loader=Loader)
        for elem in data:
            tmp.key = elem
            try:
                tmp.value = data[elem]['val']
                tmp.set_is_final(data[elem]['final'])
            except TypeError:
                tmp.value = data[elem]
            except KeyError:
                tmp.final = False
            configs.append(tmp)
            tmp = ConfigValue()
        return cls(configs)

    def to_xml(self):
        """Return an XML representation of this Config"""
        out = ["<configuration>"]
        for config in self.configs:
            out.append("\t<property>");
            out.append(config.to_xml(2))
            out.append("\t</property>")
        out.append("</configuration>")
        return '\n'.join(out)

# Execute a small demo if run as a script
if __name__ == "__main__":
    import sys

    if (len(sys.argv) < 2):
        print("Usage: " + __file__ + " <xml-file>")
        exit(1)

    fname = sys.argv[1]
    if fname.split('.')[-1] == "xml":
        conf = Config.from_xml(fname)
    else:
        conf = Config.from_yaml(fname)

    print(conf)
    print(conf.to_xml())