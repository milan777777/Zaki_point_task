import requests


class Planet:
    def __init__(self, planet_name, dist):
        self.planet_name = planet_name
        self.dist = dist
        print(f"{self.planet_name} is {self.dist} million km from Sun")


api_url = "https://api.le-systeme-solaire.net/rest/bodies/"


response = requests.get(api_url)
bodies = response.json()["bodies"]

planet_data = []

for body in bodies:
    if body.get("isPlanet"):
        axis = body.get("semimajorAxis")
        if axis:
            dist = axis / 1000000 
            name = body.get("englishName", "No Name")
            planet_data.append({'name': name, 'dist': dist})
            Planet(name, dist)

