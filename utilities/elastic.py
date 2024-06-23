from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk


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

    # Disable refresh
    es.indices.refresh(index=index_name, body={"refresh_interval": "-1"})

    # Use parallel bulk with a larger chunk size
    a = list(parallel_bulk(es, doc_generator(df), chunk_size=1000, thread_count=4))

    # Re-enable refresh
    es.indices.refresh(index=index_name, body={"refresh_interval": "1s"})

    print(a)
