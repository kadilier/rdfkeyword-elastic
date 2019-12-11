import argparse
import glob
import multiprocessing
import os
import re
import sys
from timeit import default_timer as timer

import el_controller


def init_ext_properties(pfile):
    if not os.path.isfile(pfile):
        print('Error, ' + '\'' + pfile + '\'' + ' is not proper properties file')
        sys.exit(-1)

    properties_map = {}
    with open(pfile) as fp:
        line = fp.readline()
        while line:
            contents = re.split("\t", line.replace("\n", ""))

            if len(contents) != 2:
                print('Error, ' + '\'' + pfile + '\'' + ' is not proper properties file')
                sys.exit(-1)

            prop_name = contents[0]
            property = contents[1]
            properties_map[prop_name] = property
            line = fp.readline()

    print('reading properties: ', properties_map)
    return properties_map


# # setting up arguments parser
parser = argparse.ArgumentParser(description='\'Indexer for generating the extended index\'')
parser.add_argument('-rdfD', help='"specify as ARG the directory of RDF data input(.ttl files)', required=True)
parser.add_argument('-pindex', help='specify the name of the properties index', required=True)
parser.add_argument('-eindex', help='specify the name of the new extended index', required=True)
parser.add_argument('-pfile', help='specify including properties file', required=True)
parser.add_argument('-o', help='include object\'s properties? (0 : false, 1 : true)', required=True)

args = vars(parser.parse_args())
rdf_folder = args['rdfD']
prop_index = args['pindex']
ext_index = args['eindex']
obj_incl = int(args['o'])
properties_map = init_ext_properties(args['pfile'])

# known namespaces - resources
name_spaces = []
name_spaces.append("http://dbpedia.org/resource")

# initialize elastic - expects a localhost binding in port 9200
el_controller.init('localhost', 9200)
bulk_size = 3500

####################################################
def get_name_space(triple_part, pre_flag):
    if pre_flag:
        n_space = triple_part.rsplit('#', 1)[0]
    else:
        n_space = triple_part.rsplit('/', 1)[0]

    return n_space


def is_resource(full_uri):
    for nspace in name_spaces:
        if full_uri == nspace:
            return True
    return False


def get_property(entity):
    return \
        {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "resource_terms": "" + entity
                        }
                    }
                }
            }
        }

####################################################
def parse_rdf_folder(rdf_folder):
    iter = 0
    iters_took = 0
    bulk_actions = []

    print("--" + rdf_folder + ": started")

    for ttl_file in glob.glob(rdf_folder + '/*.ttl'):
        with open(ttl_file) as fp:

            prop_maps = {}

            line = fp.readline()
            while line:

                if "<" not in line:
                    line = fp.readline()
                    continue

                line = line.replace("<", "").replace(">", "").replace("\n", "")
                contents = line.split(" ", 2)

                if len(contents) < 3:
                    line = fp.readline()
                    continue

                # handle subject
                sub_keywords = contents[0].rsplit('/', 1)[-1].replace(":", "")
                sub_nspace = get_name_space(contents[0], False)

                # handle predicate
                if "#" not in contents[1]:
                    pred_keywords = contents[1].rsplit('/', 1)[-1].replace(":", "")
                    pred_nspace = get_name_space(contents[1], False)
                else:
                    pred_keywords = contents[1].rsplit('#', 1)[-1].replace(":", "")
                    pred_nspace = get_name_space(contents[1], True)

                # handle object
                if "\"" in contents[2]:
                    obj_keywords = contents[2].replace("\"", " ")[:-2]
                    obj_nspace = ""
                elif "/" in contents[2]:
                    obj_keywords = contents[2].rsplit('/', 1)[-1].replace(":", "")[:-2]
                    obj_nspace = get_name_space(contents[2], False)
                elif "#" in contents[2]:
                    obj_keywords = contents[2].rsplit('#', 1)[-1].replace(":", "")[:-2]

                # create elastic triple-doc
                doc = {}
                doc["subjectKeywords"] = sub_keywords
                doc["predicateKeywords"] = pred_keywords
                doc["objectKeywords"] = obj_keywords
                doc["subjectNspaceKeys"] = sub_nspace
                doc["predicateNspaceKeys"] = pred_nspace
                doc["objectNspaceKeys"] = obj_nspace

                ###### get all properties described in pfile ######
                # retrieve all subject's properties

                for prop_name in properties_map.keys():

                    if prop_maps.__contains__(sub_keywords):
                        doc[prop_name + "_sub"] = prop_maps[sub_keywords]

                    else:
                        prop_res = el_controller.search(prop_index, '', 150, get_property(sub_keywords))
                        doc[prop_name + "_sub"] = []

                        for prop_hit in prop_res['hits']['hits']:
                            doc[prop_name + "_sub"].append(" " + prop_hit["_source"][prop_name])

                        prop_maps[sub_keywords] = doc[prop_name + "_sub"]

                # retrieve all object's properties (if -o is 1)
                if obj_incl == 1 and is_resource(obj_nspace):

                    for prop_name in properties_map.keys():

                        if prop_maps.__contains__(obj_keywords):
                            doc[prop_name + "_obj"] = prop_maps[obj_keywords]

                        else:

                            prop_res = el_controller.search(prop_index, '', 150, get_property(obj_keywords))
                            doc[prop_name + "_obj"] = []
                            for prop_hit in prop_res['hits']['hits']:
                                doc[prop_name + "_obj"].append(" " + prop_hit["_source"][prop_name])

                            prop_maps[obj_keywords] = doc[prop_name + "_obj"]

                ############

                # add insert action
                action = {
                    "_index": ext_index,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_source": doc
                }

                bulk_actions.append(action)
                if len(bulk_actions) > bulk_size:
                    el_controller.bulk_action(bulk_actions)
                    del bulk_actions[0:len(bulk_actions)]

                iter += 1
                if iter % 1000000 == 0:
                    print("Iter: ", iter, " -- " + rdf_folder)
                line = fp.readline()

    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)

    print("--" + rdf_folder + ": finished")
####################################################


#
ttlfolders = []
for ttl_folder in os.listdir(rdf_folder):
    ttl_folder = rdf_folder + "/" + ttl_folder
    if os.path.isdir(ttl_folder):
        ttlfolders += [os.path.join(ttl_folder, f) for f in os.listdir(ttl_folder)]

start = timer()
p = multiprocessing.Pool(8)
p.map(parse_rdf_folder, ttlfolders)

end = timer()
print("elapsed time: ", (end - start))
