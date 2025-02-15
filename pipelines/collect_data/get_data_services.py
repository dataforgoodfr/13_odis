# Calculer l'indicateur Nombre de prestataires de services par service

# code commune : GEO
# libellé commune ou ARM : ?
# réseau de proximité pôle emploi : FACILITY_TYPE = A122
# implantations France Services : A128
# banques, caisses d'épargne : A203
# bureau de poste : A206
# agence postale : A208
# école de conduite : A304
# coiffure : A501
# agence de travail temporaire : A503
# restaurant - restauration rapide : A504
# région : ?
# département : ?

# Pour libellé, région et département, investiguer API Metadata (Code Officiel Géographique)

import pandas as pd
import json
import requests

api_url = "https://api.insee.fr/melodi/data/DS_BPE?GEO=COM-33063"
# Dénombrement des équipements (sport, services, santé...)
# source BPE Base Permanente des Equipements, données 2023

get_data = requests.get(api_url, verify=False)
data_from_net = get_data.content
data = json.loads(data_from_net)

# Extraction des informations du jeu de données
title = data["title"]["fr"]
identifier = data["identifier"]

# Extraction des observations du jeu de données filtré, sur lesquelles on va boucler
observations = data["observations"]
extracted_data = []

# Boucle de lecture des observations dans le json
for obs in observations:
    dimensions = obs["dimensions"]
    
    # Suivant les jeux de données attributes est présent ou non
    if "attributes" in obs:
        attributes = obs["attributes"]
    else:
        attributes = None

    # Suivant les jeux de données value peut être absent
    if "value" in obs["measures"]["OBS_VALUE_NIVEAU"]:
        measures = obs["measures"]["OBS_VALUE_NIVEAU"]["value"]
    else:
        measures = None

    # on rassemble tout dans un objet
    if "attributes" in obs:
        combined_data = {**dimensions, **attributes, "OBS_VALUE_NIVEAU": measures}
    else:
        combined_data = {**dimensions, "OBS_VALUE_NIVEAU": measures}

    extracted_data.append(combined_data)

# Création d'un dataframe python
df = pd.DataFrame(extracted_data) 

list_facility = ['A122','A128','A203','A206','A208','A304','A501','A503','A504']

df = df[df['FACILITY_TYPE'].isin(list_facility)]

print(f"Jeu de données : {identifier} \nTitre : {title} ")
print(f"Nombre des lignes : {df.shape[0]}")

df.to_csv('./data/imports/services/ex_services_Bordeaux.csv', index=False)