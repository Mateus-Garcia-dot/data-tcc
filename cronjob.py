import httpx
from datetime import datetime
import pytz
import pandas as pd
from datetime import datetime
from utilities.elastic import get_elastic_client
from utilities.elastic import bulk_insert

es = get_elastic_client()

client = httpx.Client(base_url="https://transporteservico.urbs.curitiba.pr.gov.br")

response = client.get('/getVeiculos.php?c=98ad8')

veiculos = response.json()

veiculos = [value for _, value in veiculos.items()]

df = pd.DataFrame(veiculos)

df['date'] = datetime.now(pytz.utc)

df['coords'] = df.apply(lambda row: f"{row['LAT']},{row['LON']}", axis=1)

df.drop(columns=['LAT', 'LON'], inplace=True)

index_name = "veiculos"

index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "COD": { "type": "keyword" },
            "REFRESH": { "type": "date", "format": "HH:mm" },
            "CODIGOLINHA": { "type": "keyword" },
            "ADAPT": { "type": "integer" },
            "TIPO_VEIC": { "type": "integer" },
            "TABELA": { "type": "integer" },
            "SITUACAO": { "type": "keyword" },
            "SITUACAO2": { "type": "keyword" },
            "SENT": { "type": "keyword" },
            "TCOUNT": { "type": "integer" },
            "date": { 
                "type": "date" },
            "coords": { "type": "geo_point" }
        }
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_settings)

print(df)

bulk_insert(es, df, index_name)