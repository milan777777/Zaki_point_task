import requests
import logging
import argparse

logger = logging.getLogger("ETL")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Planet:
    def __init__(self, name, distance):
        self.name = name
        self.distance = distance 
    
    def __repr__(self):
        return f"{self.name}: {self.distance} million km"

def get_planetary_data():
    url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(url)
    data = response.json()
    planets = []
    
    for body in data.get("bodies", []):  
        if body.get("isPlanet"):
            distance = body.get("semimajorAxis", 0) / 1000000
            planets.append(Planet(
                name=body.get("englishName", "Unknown"),
                distance=distance
            ))
    return planets
                                                                                                                                                
def insertion_sort(planets, ascending=True):
    for i in range(1, len(planets)):
        current = planets[i]
        j = i - 1
        if ascending:
            while j >= 0 and current.distance < planets[j].distance:
                planets[j + 1] = planets[j]
                j -= 1
        else:
            while j >= 0 and current.distance > planets[j].distance:
                planets[j + 1] = planets[j]
                j -= 1
        planets[j + 1] = current
    return planets

def bubble_sort(planets, ascending=True):
    n = len(planets)
    for i in range(n):
        for j in range(0, n-i-1):
            if ascending:
                if planets[j].distance > planets[j+1].distance:
                    planets[j], planets[j+1] = planets[j+1], planets[j]
            else:
                if planets[j].distance < planets[j+1].distance:
                    planets[j], planets[j+1] = planets[j+1], planets[j]
    return planets

def main():
    parser = argparse.ArgumentParser(description="Display solar system planets sorted by distance from Sun")
    parser.add_argument("--sort", required=True,
                      help="apiA for Ascending, apiD for Descending")
    parser.add_argument("--algorithm", choices=['insertion', 'bubble'], default='insertion',
                      help="Sorting algorithm: insertion or bubble")
    args = parser.parse_args()

   
    sort_arg = args.sort.lower()
    if sort_arg.endswith('a'):
        ascending = True
    elif sort_arg.endswith('d'):
        ascending = False

    planets = get_planetary_data()

    if args.algorithm == 'bubble':
        sorted_planets = bubble_sort(planets, ascending)
    else:
        sorted_planets = insertion_sort(planets, ascending)

    for planet in sorted_planets:
        logger.info(planet)

if __name__ == "__main__":
    main()
