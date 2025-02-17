# App Reporter Data Pipeline

Este repositório contém exemplos para a ingestão de dados no projeto `app-reporter` da Buildaz, designado para consumir reviews de aplicativos da App Store (iOS) e Google Play Store (Android) para fins de *data-driven insights*.

## Passo a passo (*step by step*)

### Android

1. O notebook `ingestion.ipynb` fará uso das seguintes bibliotecas:

```python
import pandas as pd
import serpapi
```
Para mais informações sobre a documentação do SerpAPI, confira [este link](https://serpapi.com/google-play-product-api).

2. Defina a variável `MARKETS` como uma lista de tuplas, onde cada tupla corresponde ao país e à língua, respectivamente, sobre a qual será feita a pesquisa. Confira nestes links como definir [o país](https://serpapi.com/google-countries) e [a língua](https://serpapi.com/google-languages) de acordo com os parâmetros do Google. Defina também a variável `APP_ID` com o ID do aplicativo no Google Play Store.

Exemplo: `https://play.google.com/store/apps/details?id=com.mojang.minecraftpe&hl=pt_BR`

`APP_ID` → `com.mojang.minecraftpe`

3. Defina o `client` SerpAPI, com a API Key.

```python
client = serpapi.Client(api_key='INSERT API KEY HERE')
```

4. A função de ingestão irá buscar por reviews feitas desde o dia 01 de janeiro de 2024, iterando sobre os `MARKETS` definidos para o `APP_ID`.

5. Após isso, defina a variável `PROVIDER` com o nome da empresa, e o seu `PEER_GROUP` (este sempre em caixa alta). Consulte os dados no Google BigQuery para mais informações.

6. Salvo os dados em arquivo CSV, execute o script `slice_df.py`, definindo a variável `FILE_NAMES` com os prefixos dos arquivos no formato `[APP_ID].[país]-[língua]` (ex.: `br.com.lojasrenner.pt-br`). O script `slice_df` irá particionar o arquivo CSV em arquivos de até 500 linhas.

7. Feito isto, execute o script `to_gbq.py`, definindo as seguintes variáveis:

```python
MARKETS = {
    'pt-br': 17 # número de arquivos gerados pelo script `slice_df.py`
}

APP_ID = 'br.com.lojasrenner'
```

Este script fará a ingestão dos dados para o Google BigQuery.

### iOS

2. Defina a variável `MARKETS` como uma lista de strings, onde cada string corresponde ao país sobre o qual será feito a busca. Para mais informações sobre como definir os países para a App Store, veja [este link](https://serpapi.com/apple-regions). Em seguida, defina a variável APP_ID, com o ID do aplicativo na App Store.

Exemplo: `https://apps.apple.com/br/app/lojas-renner-comprar-roupas/id567763947`

`APP_ID` → `567763947`

3. Defina o `client` SerpAPI, com a API Key.

```python
client = serpapi.Client(api_key='INSERT API KEY HERE')
```

4. A função de ingestão irá buscar por reviews feitas no ano de 2024 ou 2025, iterando sobre os `MARKETS` definidos para o `APP_ID`.
17ecute o script `slice_df.py`, definindo a variável `FILE_NAMES` com os prefixos dos arquivos no formato `[APP_ID].[país]` (ex.: `567763947.br`). O script `slice_df` irá particionar o arquivo CSV em arquivos de até 500 linhas.

7. Feito isto, execute o script `to_gbq.py`, definindo as seguintes variáveis:

```python
MARKETS = {
    'br': 4 # número de arquivos gerados pelo script `slice_df.py`
}

APP_ID = 'br.com.lojasrenner'
```

Este script fará a ingestão dos dados para o Google BigQuery.