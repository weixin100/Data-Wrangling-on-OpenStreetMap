

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema
from Audit_Street import mapping_street, update_street
from Audit_Postcodes import mapping_postcode, update_postcode

OSM_PATH = "Stanford.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
TWO_COLONS = re.compile(r'^([a-z]|_)+:([a-z]|_)+:([a-z]|_)+')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

def clean(key,v):
    if 'street' in key.split(':'):
        value = update_street(v, mapping_street)
    elif 'postcode' in key.split(':'):
        value = update_postcode(v, mapping_postcode)
    else:
        value = v
    if value != v:
        print(v, '==>', value)
    return value
    
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    
    node_tags = []
    way_tags = [] # Handle secondary tags the same way for both node and way elements


    
    if element.tag == 'node':
        
        for attrib, value in element.attrib.items():
            if attrib in node_attr_fields:
                node_attribs[attrib] = value
        
        for child in element:
            if child.tag == 'tag':
                node_tags_t = {}
                if PROBLEMCHARS.search(child.attrib['k']):
                    continue
                elif TWO_COLONS.match(child.attrib['k']):
                    node_tags_t['id'] = element.attrib['id']
                    node_tags_t['key'] = child.attrib['k'].split(':')[1] + ':' + \
                             child.attrib['k'].split(':')[2]
                    node_tags_t['value'] = clean(node_tags_t['key'],child.attrib['v'])
                    node_tags_t['type'] = child.attrib['k'].split(':')[0]
                    
                elif LOWER_COLON.search(child.attrib['k']):
                    node_tags_t['id'] = element.attrib['id']
                    node_tags_t['key'] = child.attrib['k'].split(':')[1]
                    node_tags_t['value'] = clean(node_tags_t['key'],child.attrib['v'])
                    node_tags_t['type'] = child.attrib['k'].split(':')[0]
                    
                else:
                    node_tags_t['id'] = element.attrib['id']
                    node_tags_t['key'] = child.attrib['k']
                    node_tags_t['value'] = clean(node_tags_t['key'],child.attrib['v'])
                    node_tags_t['type'] = 'regular'   
                    
                node_tags.append(node_tags_t) 
                            
        return {'node': node_attribs, 'node_tags': node_tags}
    
    elif element.tag == 'way':
        for attrib, value in element.attrib.items():
            if attrib in way_attr_fields:
                way_attribs[attrib] = value
        position_t = 0
        
        for child in element:
            if child.tag == 'tag':
                way_tags_t = {}
                if PROBLEMCHARS.search(child.attrib['k']):
                    continue
                elif TWO_COLONS.match(child.attrib['k']):
                    way_tags_t['id'] = element.attrib['id']
                    way_tags_t['key'] = child.attrib['k'].split(':')[1] + ':' + \
                             child.attrib['k'].split(':')[2]
                    way_tags_t['value'] = clean(way_tags_t['key'],child.attrib['v'])
                    way_tags_t['type'] = child.attrib['k'].split(':')[0]
                    
                elif LOWER_COLON.search(child.attrib['k']):
                    way_tags_t['id'] = element.attrib['id']
                    way_tags_t['key'] = child.attrib['k'].split(':')[1]
                    way_tags_t['value'] = clean(way_tags_t['key'],child.attrib['v'])
                    way_tags_t['type'] = child.attrib['k'].split(':')[0]
                    
                else:
                    way_tags_t['id'] = element.attrib['id']
                    way_tags_t['key'] = child.attrib['k']
                    way_tags_t['value'] = clean(way_tags_t['key'],child.attrib['v'])
                    way_tags_t['type'] = 'regular'   
                    
                way_tags.append(way_tags_t)
                
            elif child.tag == 'nd':
                way_nodes_t = {}
                way_nodes_t['id'] = element.attrib['id']
                way_nodes_t['node_id'] = child.attrib['ref']
                way_nodes_t['position'] = position_t
                position_t += 1
                
                way_nodes.append(way_nodes_t)
          

        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': way_tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(iter(validator.errors.items()))
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, str) else v) for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""
    
    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()
        
        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=True)
