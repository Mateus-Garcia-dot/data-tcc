from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk



def get_elastic_client():
    client = Elasticsearch(
        "http://elastic.tccurbstads.com:80",
        verify_certs=False,
    )
    return client



def bulk_insert(es, df, index_name):
    def doc_generator(df):
        for index, row in df.iterrows():
            yield {
                "_index": index_name,
                "_source": row.to_dict(),
            }

    a = bulk(es, doc_generator(df))
    print(a)