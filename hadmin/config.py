from xml.etree import ElementTree as ET
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class Config:
    """ Holds a Hadoop configuration.
    
    Config holds a Hadoop configuration as a two-dimensional map. It
    is organized with the first dimension as the configuration key. The
    second dimension contains two elements: value and final.

    Config allows for returning either an XML or YAML representation.
    Future work will be to include type-checking and other potential
    validation based on a rules file, although this will be in a different
    class, probably ConfigValidator or something.

    """

    @classmethod
    def from_xml(cls, filename):
        """ Parses Hadoop XML and fill a two-dimensonal map. """

        conf = cls()
        key = ''
        val = ''
        fnl = False

        for event, elem in ET.iterparse(filename):
            if elem.tag is 'name':
                if len(key) > 0:
                    conf[key][Config.val_tag] = val
                    conf[key][Config.fnl_tag] = fnl
                key = elem.text
                val = ''
                fnl = False
            elif elem.tag is 'value':
                val = elem.text
            elif elem.tag is 'final':
                fnl = elem.text
        conf.validate()
        return conf

    @classmethod
    def from_yaml(cls, filename):
        """Parse YAML and fill two-dimensional map."""

        conf = cls()
        key = ''
        val = ''
        fnl = ''

        f = open(filename, "r")
        data = load(f, Loader=Loader)

        for elem in data:
            key = elem
            try:
                val = data[elem][Config.val_tag]
                fnl = data[elem][Config.fnl_tag]
            except TypeError:
                val = data[elem]
                fnl = False
            except KeyError:
                print("KeyError in strange place... possibly malformed input")
                quit(1)

            conf[key, Config.val_tag] = val
            conf[key, Config.fnl_tag] = fnl
        conf.validate()
        return conf

    def to_xml(self):
        """ Return an XML representation of this Config """
        out = ["<configuration>"]
        for key in sorted(self.conf.keys()):
            out.append("\t<property>");
            out.append("\t\t<name>" + key + "</name>")
            out.append("\t\t<value>" + self[key, Config.val_tag] + "</value>")
            fnl = str(self[key, Config.fnl_tag]).lower()
            out.append("\t\t<final>" + fnl + "</final>")
            out.append("\t</property>")
        out.append("</configuration>")
        return '\n'.join(out)

    def add_key(self, key):
        """ Adds a new config key if it doesn't already exist. This
        is mostly to be used internally """

        if key not in self.conf.keys():
            self.conf[key] = dict()
            self.conf[key][Config.val_tag] = ""
            self.conf[key][Config.fnl_tag] = False

    def __init__(self):
        """ Takes in a two-dimensional dict of Hadoop configuration """

        self.conf = dict()

    def __str__(self):
        """ Returns a string representation. Debugging only """

        out = []
        for key in sorted(self.conf.keys()):
            tmp = []
            if self[key, Config.fnl_tag]:
                tmp.append('final')
            tmp.append('\t')
            tmp.append(key)
            tmp.append(':\t')
            tmp.append(self[key, Config.val_tag])
            out.append(''.join(tmp))
        return '\n'.join(out)

    def __getitem__(self, key):
        """ This documentation should be better """

        sub_key = Config.val_tag

        if type(key) is tuple:
            sub_key = key[1]
            key = key[0]

        if key not in self.conf.keys():
            raise KeyError("Key " + key + " doesn't exist in the config")

        return self.conf[key][sub_key]

    def __setitem__(self, key, value):
        """ Sets values """

        sub_key = Config.val_tag

        if type(key) is tuple:
            sub_key = key[1]
            key = key[0]

        if sub_key not in Config.subtags:
            raise KeyError("You cannot edit the " + sub_key + " attribute.")

        if key not in self.conf.keys():
            self.add_key(key)

        if sub_key == Config.val_tag:
            self.conf[key][sub_key] = str(value)
        elif sub_key == Config.fnl_tag:
            self.conf[key][sub_key] = bool(value)

    val_tag = 'value'
    fnl_tag = 'final'
    subtags = (val_tag, fnl_tag)

# Execute a small demo if run as a script
if __name__ == "__main__":
    import sys

    if len(sys.argv) is not 2:
        print("Usage: " + __file__ + " <file>")
        exit(1)

    fname = sys.argv[1]
    if fname.split('.')[-1] is 'xml':
        conf = Config.from_xml(fname)
    else:
        conf = Config.from_yaml(fname)

    print('Printing Hadmin representation:')
    print(conf)
    print('Printing XML representation:')
    print(conf.to_xml())
