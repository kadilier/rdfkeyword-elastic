Creates an index over **RDF** data for keyword search, with classic IR techniques, upon triple-based  documents using **Elasticsearch** (ES).

Models two different indexing perspectives in both of which each ES doc represents a triple.

1. *baseline* : only  makes use  of  the  information  that  exists  in  the  triple’s  three  components  (subject, predicate, object). In case the value of one of the components is a URI, the URI is tokenized into keywords.

2. *extended* : extends baseline document by including additional information for each triple component (if it is a resource - URI). Additional information corresponds to values of properties that can be given as input (e.g. *rdfs:comment*).

RDF files are expected in the form of triples (.ttl) in the following structure:
rdf_dataset
```
├── part1
│   ├── file1.ttl
│   ├── file2.ttl
├── part2
│   ├── file1.ttl
..
```

Below are detailed instructions for reproducing our setup for indexing the 'DBpedia dump/2015-10' following the 'Dbpedia-Entity' collection test described in https://iai-group.github.io/DBpedia-Entity/. Note, that for the *extended* index we select the *rdfs:comment* property for enriching each document.

### Deploy elasticsearch instance and create mappings
  Download and unzip elastic package (version used: elastic-6.8.0). Run an elastic instance:
  ```
  ./elasticsearch-6.8.0/bin/elasticsearch&
  ```
  instance by default binds to locahost:9200. Next, create mapping schemas of *baseline*, *extended* and *properties* indexes (found inside the *mappings* folder) using the curl command:
  ```
  curl -XPUT "http://localhost:9200/index-name" -H 'Content-Type: application/json' -d'
  ```
  ### Run baseline & properties indexer
  Script in *tdoc_base.py* parses the input RDF files and creates both *baseline* and *properties* indexes based on the above mapping shcemas. Note, properties index is used for enriching the *baseline* and creating later the *extended* index.
  
  First, install requirements as described in requirements.txt:
  ```
  pip install -r requirements.txt
  ```
  
  Next, run *tdoc_base.py* with the following configuration:
  ```
  python tdoc_base.py -rdfD rdf_dataset_dir -bindex baseline -ext 1 -pfile rdfs_comment
  ```
  
  ### Run extended indexer
  Lastly, create the extended index using *tdoc_ext.py* script as following:
  ```
  python3 tdoc_ext.py -rdfD rdf_dataset -pindex comment_values_index -eindex ext_index -pfile rdfs_comment
  ```
  
  


