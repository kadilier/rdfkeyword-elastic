# Keyword search over RDF using Elasticsearch

Creates an index over **RDF** data for keyword search, with classic IR techniques, upon triple-based  documents using **Elasticsearch** (ES).

Models two different indexing perspectives in both of which each ES doc represents a triple.

1. *baseline* : only  makes use  of  the  information  that  exists  in  the  triple’s  three  components  (subject, predicate, object). In case the value of one of the components is a URI, the URI is tokenized into keywords.

2. *extended* : extends baseline document by including additional information for each triple component (if it is a resource - URI). Additional information corresponds to values of properties that can be given as input (e.g. *rdfs:comment*).

Below are detailed instructions for reproducing our setup for indexing the 'DBpedia dump/2015-10' following the 'Dbpedia-Entity' test collection 
described in https://iai-group.github.io/DBpedia-Entity/. 
In this work, we built two indexes called *baseline* and *extended*. First contains keywords extracted from each triple component while for the second we chose the values of 
property *rdfs:comment* as  extra fields for each triple. Note, an extra index called *comment* is built which contains the values of the specified property for each resource.

## Setup & installation

Download 'DBpedia dump/2015-10' from here: http://downloads.dbpedia.org/2015-10/.
After removing duplicates we end up with a dataset containing around 400m triples. RDF files are expected to be in the form of triples (.ttl) and must be organized in the following structure:
```
rdf_dataset
├── part1
│   ├── file1.ttl
│   ├── file2.ttl
├── part2
│   ├── file1.ttl
...
``` 

Download an elastic package  (version used: elastic-6.8.0). After extracting all contents, start an instance:
```
  ./elasticsearch-6.8.0/bin/elasticsearch&
```
Instance by default binds to locahost:9200, check *elasticsearch-6.8.0/config/elasticsearch.yml* and *elasticsearch-6.8.0/config/jvm.options* for further configurations.

Next, install all requirements needed for our scripts as described in requirements.txt:
 ```
  pip install -r requirements.txt
```

## Running indexers 
First, create a mapping schema for each of our indexes (*baseline*, *extended* and *comment*) as found in */res/mappings* folder. Use the curl command:
  ```
  curl -XPUT "http://localhost:9200/index-name" -H 'Content-Type: application/json' -d'
  ```

  ### Create baseline & comment indexes
  The *tdoc_base.py* script parses input RDF files and creates both *baseline* and *comment* indexes following the above mapping schemas.
   
  Run script as following:
  ```
  python tdoc_base.py -rdfD rdf_dataset -bindex baseline -pindex comment -pfile rdfs_comment
  ```
  where *-pfile* expects as input a tab-separated file containing the desired properties (in our case *rdfs:comment*) following a *key,value* format such as:
   ```
    rdfs_comment    http://www.w3.org/2000/01/rdf-schema#comment
    dbo_abstract    http://dbpedia.org/ontology/abstract
   ```

  ### Create extended index
  Lastly, create the extended index using the *tdoc_ext.py* script as following:
  ```
  python tdoc_ext.py -rdfD rdf_dataset -pindex comment -eindex extended -pfile rdfs_comment
  ```
  
  


