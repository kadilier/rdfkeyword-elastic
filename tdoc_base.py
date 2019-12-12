import argparse
import glob
import multiprocessing
import os
import re
import sys
from timeit import default_timer as timer

import el_controller


# initialize input properties file (pfile)
def init_prop_file(pfile):
    if not os.path.isfile(pfile):
        print('Error, ' + '\'' + pfile + '\'' + ' is not a proper properties file')
        sys.exit(-1)

    properties_map = {}
    with open(pfile) as fp:
        line = fp.readline()
        while line:
            contents = re.split("\t", line.replace("\n", ""))

            if len(contents) != 2:
                print('Error, ' + '\'' + pfile + '\'' + ' is not a proper properties file')
                sys.exit(-1)

            prop_name = contents[0]
            property = contents[1]
            properties_map[prop_name] = property
            line = fp.readline()

    return properties_map


# extract name-space from an input URI
def get_name_space(triple_part, pre_flag):
    if pre_flag:
        n_space = triple_part.rsplit('#', 1)[0]
    else:
        n_space = triple_part.rsplit('/', 1)[0]

    return n_space

def is_resource(full_uri):
    return full_uri in name_spaces

def index_rdf_folder(input_folder):
    # bulk config - empirically set to 3500
    bulk_size = 3500
    prop_bulk_size = 3500
    bulk_actions = []
    prop_bulk_actions = []

    iter = 0
    print("--" + input_folder + ": started")

    # parse each .ttl file inside input folder
    for ttl_file in glob.glob(input_folder + '/*.ttl'):
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

                        # create a property - document
                        prop_doc = {"resource_terms": sub_keywords, field_prop: obj_keywords}

                        # add insert action
                        prop_action = {
                            "_index": prop_index,
                            '_op_type': 'index',
                            "_type": "_doc",
                            "_source": prop_doc
                        }

                        prop_bulk_actions.append(prop_action)
                        if len(prop_bulk_actions) > prop_bulk_size:
                            el_controller.bulk_action(prop_bulk_actions)
                            del prop_bulk_actions[0:len(prop_bulk_actions)]

                # create a triple - document
                doc = {"subjectKeywords": sub_keywords, "predicateKeywords": pred_keywords,
                       "objectKeywords": obj_keywords, "subjectNspaceKeys": sub_nspace,
                       "predicateNspaceKeys": pred_nspace, "objectNspaceKeys": obj_nspace}

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
                    print("Iter: ", iter, " -- " + input_folder)
                line = fp.readline()
                ####

    # flush any action that is left inside the bulk actions
    el_controller.bulk_action(bulk_actions)
    el_controller.bulk_action(prop_bulk_actions)

    print("--" + input_folder + ": finished")


# known namespaces - resources (manually maintained)
name_spaces = set()
name_spaces.add("http://dbpedia.org/resource")

def main():
    # setting up arguments parser
    parser = argparse.ArgumentParser(description='\'Indexer for generating the baseline index\'')
    parser.add_argument('-rdfD', help='"specify the directory of RDF data input (.ttl files)', required=True)
    parser.add_argument('-bindex', help='specify the name of the elastic baseline index', required=True)
    parser.add_argument('-p', help='specify whether property mode is enabled (0, 1). If 1 creates properties index'
                                   'based on -pindex & -pfile.',
                        required=True)
    parser.add_argument('-pindex', help='when property mode is on, specify a properties index name to be created.',
                        required=False)
    parser.add_argument('-pfile', help='specify including properties file',
                        required=False)

    args = vars(parser.parse_args())

    # initialize elastic - expects a localhost binding in port 9200
    el_controller.init('localhost', 9200)

    global base_index
    global prop_index
    base_index = args['bindex']
    rdf_folder = args['rdfD']
    prop_index = args['pindex']

    global ext_mode
    ext_mode = int(args['p'])
    if ext_mode == 1:
        global ext_properties
        ext_properties = init_prop_file(args['pfile'])

    # deploy index instances (currently set manually to 12)
    ttl_folders = []
    for ttl_folder in os.listdir(rdf_folder):
        ttl_folder = rdf_folder + "/" + ttl_folder
        if os.path.isdir(ttl_folder):
            ttl_folders += [os.path.join(ttl_folder, f) for f in os.listdir(ttl_folder)]

    start = timer()
    p = multiprocessing.Pool(12)
    p.map(index_rdf_folder, ttl_folders)

    end = timer()
    print("elapsed time: ", (end - start))


if __name__ == "__main__":
    main()
