"""
HAdmin is an API and command line tool to configure the queues of Hadoop's
CapacityScheduler.
"""


import xml.etree.ElementTree as ET


class HXML:
    """
    A wrapper around etree specific to Hadoop's XML format.

    Provides an easy way to deal with Hadoop's XML so that calling classes
    only worry about Hadoop config keys and values, and not dealing with all
    the cruft. Unless you're crazy, avoid using this class directly and instead
    use a class such as CapacityScheduler to fulfill your needs.
    """

    DEFAULT_EMPTY_TREE = ET.fromstring('<configuration></configuration>')

    def __init__(self, etree=DEFAULT_EMPTY_TREE):
        self.tree = etree

    @classmethod
    def from_str(cls, string):
        """ Construct from a raw XML string. """
        ret = cls(ET.fromstring(string))
        return ret

    @classmethod
    def from_file(cls, fname):
        """ Construct from an XML file. """
        ret = cls(ET.parse(fname).getroot())
        return ret

    def __getitem__(self, prop):
        """ Retrieves a config value. """
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                return node.find('value').text

        raise KeyError('Key ' + prop + ' not found')

    def get_or(self, prop, default):
        """
        Returns the value associated with prop or the default value
        """

        try:
            return self[prop]
        except KeyError:
            pass

        return default

    def __setitem__(self, prop, val):
        """
        Sets a config value. Please note that if the specified property
        prop does not exist, it will be created.
        """

        success = False

        if type(val) in (int, float):
            val = str(val)
        elif type(val) is bool:
            val = str(val).lower()

        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                node.find('value').text = val
                success = True

        if success is not True:
            el = ET.Element('property')
            el.text = "\n    "
            el.tail = "\n"
            name_el = ET.Element('name')
            name_el.tail = "\n    "
            name_el.text = prop
            el.append(name_el)
            val_el = ET.Element('value')
            val_el.tail = "\n  "
            val_el.text = val
            el.append(val_el)
            self.tree.append(el)

    def remove(self, prop):
        """ Deletes a property from the etree. """
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                self.tree.remove(node)

    def keys(self):
        """ Returns a list of keys. """
        ret = list()
        for node in self.tree.findall('property'):
            ret.append(node.find('name').text)
        return sorted(ret)

    def save(self, fname):
        """ Saves to a file. """
        ET.ElementTree(self.tree).write(fname)

    def merge(self, other):
        for k in other.keys():
            self[k] = other[k]


def users_from_passwd(raw):
    """ Extracts a list of users from a passwd type file. """
    users = list()
    for line in raw.split('\n'):
        tmp = line.split(':')[0]
        if len(tmp) > 0:
            users.append(tmp)

    return sorted(users)
