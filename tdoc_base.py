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
parser = argparse.ArgumentParser(description='\'Indexer for generating the baseline index\'')
parser.add_argument('-rdfD', help='"specify the directory of RDF data input (.ttl files)', required=True)
parser.add_argument('-bindex', help='specify the name of the elastic baseline index', required=True)
parser.add_argument('-ext', help='specify extended mode(0 : tdoc_base, 1 : extended with rdfs-comment/dbo-redirect)',
                    required=True)
parser.add_argument('-pindex', help='when extended mode is on, specify a properties index name', required=False)
parser.add_argument('-pfile', help='specify including properties file, later used for the extended index',
                    required=False)

args = vars(parser.parse_args())

# initialize elastic - expects a localhost binding in port 9200
el_controller.init('localhost', 9200)

# empirically set at 3500
bulk_size = 3500
prop_bulk_size = 3500

base_index = args['bindex']
rdf_folder = args['rdfD']
pindex = args['pindex']
prop_bulk_actions = []
ext_mode = int(args['ext'])

# known namespaces - resources
name_spaces = []
name_spaces.append("http://dbpedia.org/resource")

if ext_mode == 1:
    ext_properties = init_ext_properties(args['pfile'])


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

def get_property(term, p_nspace, p_keywords):
    return \
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "term": {
                                        "subjectTerms": "" + term
                                    }
                                }
                            }
                        },
                        {
                            "match_phrase": {
                                "predicateNspaceKeys": "" + p_nspace
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {"match_phrase": {"predicateKeywords": "" + p_keywords}}
                                ]
                            }
                        }

                    ]
                }
            }
        }

def parse_rdf_folder(rdf_folder):
    iter = 0
    bulk_actions = []

    print("--" + rdf_folder + ": started")

    for ttl_file in glob.glob(rdf_folder + '/*.ttl'):
        with open(ttl_file) as fp:

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

                if ext_mode == 1:

                    # if predicate-property is included in properties-files config
                    if contents[1] in ext_properties.values():

                        # get field-prop name
                        field_prop = {v: k for k, v in ext_properties.items()}[contents[1]]

                        # # check if resource-property already exists
                        # res = el_controller.search(pindex, '', 1,
                        #                            get_property(sub_keywords, pred_nspace, pred_keywords))
                        #
                        # # insert new resource-property document into properties index
                        # if len(res['hits']['hits']) == 0:

                        prop_doc = {}
                        prop_doc["resource_terms"] = sub_keywords

                        ### test if field prop == rdfs_comment

                        prop_doc[field_prop] = obj_keywords

                        # add insert action
                        prop_action = {
                            "_index": pindex,
                            '_op_type': 'index',
                            "_type": "_doc",
                            "_source": prop_doc
                        }

                        prop_bulk_actions.append(prop_action)
                        if len(prop_bulk_actions) > prop_bulk_size:
                            el_controller.bulk_action(prop_bulk_actions)
                            del prop_bulk_actions[0:len(prop_bulk_actions)]

                # # create elastic triple-doc
                doc = {}
                doc["subjectKeywords"] = sub_keywords
                doc["predicateKeywords"] = pred_keywords
                doc["objectKeywords"] = obj_keywords
                doc["subjectNspaceKeys"] = sub_nspace
                doc["predicateNspaceKeys"] = pred_nspace
                doc["objectNspaceKeys"] = obj_nspace

                # add insert action
                action = {
                    "_index": base_index,
                    '_op_type': 'index',
                    "_type": "_doc",
                    "_source": doc
                }

                bulk_actions.append(action)
                if len(bulk_actions) > bulk_size:
                    el_controller.bulk_action(bulk_actions)
                    del bulk_actions[0:len(bulk_actions)]

                #### monitor output ####
                iter += 1
                if iter % 1000000 == 0:
                    print("Iter: ", iter, " -- " + rdf_folder)
                line = fp.readline()
                ####


    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)
    el_controller.bulk_action(prop_bulk_actions)

print("--" + rdf_folder + ": finished")

####################################################

#
ttlfolders = []
for ttl_folder in os.listdir(rdf_folder):
    ttl_folder = rdf_folder + "/" + ttl_folder
    if os.path.isdir(ttl_folder):
        ttlfolders += [os.path.join(ttl_folder, f) for f in os.listdir(ttl_folder)]

start = timer()
p = multiprocessing.Pool(12)
p.map(parse_rdf_folder, ttlfolders)

end = timer()
print("elapsed time: ", (end - start))
