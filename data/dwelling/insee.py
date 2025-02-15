import requests

BASE_URL = "https://api.insee.fr/melodi/data/DS_RP_LOGEMENT_PRINC"
MAX_RESULT = 10000

def get_vacant_dwelling_by_year(year):
    if not year:
        assert False, "year must be select"
    
    url = f"{BASE_URL}?TIME_PERIOD={year}&OCS=DW_VAC&maxResult={MAX_RESULT}"
    return get_observations(url)

def get_dwelling_by_year_and_room_count(year, room_count):
    room_count_labels = ["R1","R2","R3","R4","R_GE5"]
    if not year:
        assert False, "year must be select"
    if not room_count:
        assert False, "room_count must be select"
    if room_count not in room_count_labels:
        assert False, f"room_count must be in {room_count_labels}"
    
    url = f"{BASE_URL}?TIME_PERIOD={year}&NOR={room_count}&maxResult={MAX_RESULT}"
    return get_observations(url)

def get_observations(url):
    observations = []
    isLast = False
    url = f"{url}"

    while not isLast:
        print(url)
        response = requests.get(url)

        if response.ok:
            data = response.json()
            for observation in data["observations"]:
                observations.append({
                    **observation["dimensions"],
                    "value": observation["measures"]["OBS_VALUE_NIVEAU"]["value"]
                })

        isLast = data["paging"]["isLast"]
        if "next" in data["paging"]:
            url = data["paging"]["next"]

    return observations
