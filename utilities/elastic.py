from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk



def get_elastic_client():
    ELASTIC_PASSWORD = "!@ContaTccElastic"
    client = Elasticsearch(
        "https://192.168.1.3:9200",
        verify_certs=False,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    return client



def bulk_insert(es, df, index_name):
    def doc_generator(df):
        for index, row in df.iterrows():
            yield {
                "_index": index_name,
                "_source": row.to_dict(),
            }

    bulk(es, doc_generator(df))