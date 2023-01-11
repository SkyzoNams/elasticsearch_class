#!/usr/bin/env python
import os
import json
import configparser
from elasticsearch import Elasticsearch


class ELASTIC_SEARCH_MANAGER:
    def __init__(self, index="dmu_search_01_version_filename", index_type="dmu", config_filepath=None):
        print ('in ELASTIC_SEARCH_MANAGER')
        self.index_type = index_type
        self.config_filepath = config_filepath
        self.config = self.configuration()
        self.index = index
        self.es = Elasticsearch([self.config['elasticsearch']['host']])

    def configuration(self):
        config = configparser.RawConfigParser()
        path = self.config_filepath
        if path is None:
            path = get_relative_path() + '/elastic_config/config.ini'
        if os.path.isfile(path):
            filename = path
            config.read(filename)
            return config
        return None

    def get(self, foreign_key):
        try:
            res = self.es.get(index=self.index, doc_type=self.index_type, id=foreign_key)
            return res
        except Exception as e:
            raise e

    def search(self, id):
        try:
            res = self.es.search(index=self.index, doc_type=self.index_type, body={"query": { "match" : {"_id": id}}})
            return self.get_source(res)
        except Exception as e:
            raise e


    def upsert(self, key, value, foreign_key):
        try:
            if key != "" and value is not None and value != '' and (isinstance(value, bool) or len(value) > 0):
                self.es.update(index=self.index, doc_type=self.index_type, id=foreign_key, body={'doc': {key: value},'doc_as_upsert': True})
        except Exception as e:
            print ("[ERROR] in elastic upsert() for the key: " + str(key) + " and the value: " + str(value) + " and the FK: " + str(foreign_key))
            
    def upsert_obj(self, obj, foreign_key):
        try:
            if obj is not None and len(obj) > 0:
                self.es.update(index=self.index, doc_type=self.index_type, id=foreign_key, body={'doc': obj,'doc_as_upsert': True})
        except Exception:
            print ("[ERROR] in elastic upsert_obj() for the obj: " + str(obj) + " and the FK: " + str(foreign_key))


    def upsert_json(self, json_data, foreign_key):
        for key, value in json_data.items():
            self.upsert(key, value, foreign_key)


    def upsert_all(self, json_folder):
        foreign_key = "" ## MUST BE DEFINE
        for json in os.listdir(json_folder):
            json_data = get_json(json_folder + '/' + json)
            self.upsert_json(json_data, foreign_key)



    ## must be tested
    def delete_record_by_id(self, id):
        self.es.delete(index=self.index, doc_type=self.index_type, id=id)


    ## must be tested   
    def delete_fields(self, key, value):
        self.es.delete_by_query(index=self.index, doc_type=self.index_type, body={'term': {key: value}})


    def select_all(self):
        try:
            res = self.es.search(index=self.index, doc_type=self.index_type, body={"query": { "match_all" : {}}})
            return res
        except Exception as e:
            raise e


    def get_source(self, elastic_obj):
        try:
            if len(elastic_obj['hits']['hits']) > 0:
                return elastic_obj['hits']['hits'][0]['_source']
            return {}
        except Exception as e:
            raise e


def get_relative_path():
    fileDir = os.path.dirname(os.path.abspath(__file__))
    parentDir = os.path.dirname(fileDir)
    return parentDir.replace('\\', '/')

def get_json(filename):
    with open(filename) as json_data:
        return json.load(json_data)