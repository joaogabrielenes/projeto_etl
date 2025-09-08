import requests
import pandas as pd
import os

def extract():
    url = "https://pokeapi.co/api/v2/pokemon?limit=10000"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}")
    data = response.json()
    return data["results"]


def transform(dados):
    pokemons = []

    for item in dados:
        pokemon_id = item["url"].strip("/").split("/")[-1]
        pokemons.append({
            "id": int(pokemon_id),
            "Nome Pokemon": item["name"]
        })


    df_pokemon = pd.DataFrame(pokemons)
    df_pokemon['Nome Pokemon'] = df_pokemon["Nome Pokemon"].str.upper()

    df_pokemon= df_pokemon[df_pokemon["Nome Pokemon"].str[0].isin(["B", "C","V", "R"])]
    df_pokemon = df_pokemon.sort_values(by=["id"]).reset_index(drop=True)

    
    return df_pokemon 



def load():
    dados_extraidos = extract()           
    data_resultados = transform(dados_extraidos)
    os.makedirs("data", exist_ok=True)
    data_resultados.to_parquet("data/pokemons.parquet", index=False)
    data_resultados.to_csv("data/pokemons.csv", index=False)
    print("ETL concluido")

load()