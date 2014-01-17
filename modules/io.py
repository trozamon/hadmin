"""
io.py
=====

Functions to transform from hadmin files to XML.
"""

"""
struct thing
{
	string key
	string value
	bool isFinal
}

read in hadmin file
for each line
	read first word
	if final
		mark final
		read next word and set key to word
	else
		set key to word
	endif
	read until newline and set value to the rest
endfor
"""

from xml.etree import ElementTree as ET

class ConfigValue:
    key = ""
    value = ""
    is_final = ""

    def __init__(self):
        is_final = "false"

    def __str__(self):
        ret =  "Key: " + self.key
        ret += " Value: " + self.value + " Final?: " + self.is_final
        return ret

def parse_xml(filename):
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

    return configs

def parse_hadmin(filename):
    configs = []
    tmp = ConfigValue()

    f = open(filename, "r")
    for line in f:
        line.rstrip('\n')
        if line.find("\n"):
            print("Found a newline")
            exit(2)
        arr = line.split(" ")

        if len(arr) == 3:
            if arr[0] == "final":
                tmp.is_final = "true"

            tmp.key = arr[1]
            tmp.value = arr[2]
        elif len(arr) == 2:
            tmp.key = arr[0]
            tmp.value = arr[1]
            tmp.is_final = "false"
        else:
            print("Error processing hadmin file")
            exit(1)
        configs.append(tmp)
        tmp = ConfigValue()
    return configs

if __name__ == "__main__":
    import sys

    if (len(sys.argv) < 2):
        print("Usage: " + __file__ + " <xml-file>")
        exit(1)

    fname = sys.argv[1]
    configs = []
    if fname.split('.')[-1] == "xml":
        configs = parse_xml(fname);
    else:
        configs = parse_hadmin(fname)

    for config in configs:
        print(config)
