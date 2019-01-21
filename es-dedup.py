#!/usr/bin/env python2
# The original source of this script is blog post on elastic.co:
# https://www.elastic.co/blog/how-to-find-and-remove-duplicate-documents-in-elasticsearch
import hashlib
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=["localhost"], port=32220)
dict_of_duplicate_docs = {}
page_size = 1000
index_name = "sft-nuance-2018.12.22"
# The following line defines the fields that will be
# used to determine if a document is a duplicate
keys_to_include_in_hash = ["TransactionStartDateTime", "TransactionEndDateTime", "SessionRootParentID"]
list_of_fields_to_return = keys_to_include_in_hash
# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hits):
    for item in hits:
        combined_key = ""
        for mykey in keys_to_include_in_hash:
            combined_key += str(item['_source'][mykey])
        _id = item["_id"]
        hashval = hashlib.md5(combined_key.encode('utf-8')).digest()
        # If the hashval is new, then we will create a new key
        # in the dict_of_duplicate_docs, which will be
        # assigned a value of an empty array.
        # We then immediately push the _id onto the array.
        # If hashval already exists, then
        # we will just push the new _id onto the existing array
        dict_of_duplicate_docs.setdefault(hashval, []).append(_id)
# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
def scroll_over_all_docs():
    page_number = 1
    print "Index: ", index_name, " ", "Page size: ", page_size, " ", "Page number: ", page_number
    data = es.search(index="{0}".format(index_name), size=page_size, scroll='1m', _source=list_of_fields_to_return, body={"query": {"match_all": {}}})
    # Get the scroll ID
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    # Before scroll, process current batch of hits
    populate_dict_of_duplicate_docs(data['hits']['hits'])
    while scroll_size > 0:
        page_number += 1
        print "Index: ", index_name, " ", "Page size: ", page_size, " ", "Page number: ", page_number
        data = es.scroll(scroll_id=sid, scroll='1m')
        # Process current batch of hits
        populate_dict_of_duplicate_docs(data['hits']['hits'])
        # Update the scroll ID
        sid = data['_scroll_id']
        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])
def loop_over_hashes_and_remove_duplicates():
    duplicated_count = 0
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
        if len(array_of_ids) > 1:
            duplicated_count += 1
    print "Have been found duplicated documents: ", duplicated_count 
    duplicated_count = 0
    with open(r'./es-dedup.out', 'w') as output_file:
        for hashval, array_of_ids in dict_of_duplicate_docs.items():
            if len(array_of_ids) > 1:
                duplicated_count += 1
                output_file.write("********** Duplicate docs hash={0} **********\n".format(hashval))
                # Get the documents that have mapped to the current hashval
                matching_docs = es.mget(index="{0}".format(index_name), doc_type="doc", _source=list_of_fields_to_return, body={"ids": array_of_ids})
                matching_docs['docs'].pop(0)
                for doc in matching_docs['docs']:
                    # In this example, we just print the duplicate docs.
                    # This code could be easily modified to delete duplicates
                    # here instead of printing them
                    # output_file.write("doc = {0}\n".format(doc['_id']))
                    es.delete(index="{0}".format(index_name), doc_type="doc", id="{0}".format(doc['_id']))
                if duplicated_count in [1, 10, 100] or duplicated_count % 1000 == 0:
                    print duplicated_count, " duplicated documents have been unduplicated" 
    print "\nDone, ", duplicated_count, " duplicated documents have been unduplicated\n" 
def main():
    scroll_over_all_docs()
    loop_over_hashes_and_remove_duplicates()
main()
