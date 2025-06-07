import requests
import json
import re
from msal import ConfidentialClientApplication
from datetime import datetime
import pandas as pd
from datetime import date, timedelta








BASE_ADDRESS = "https://api.signcoserv.be"
CLIENT_ID = "xxxxx"
CLIENT_SECRET = "xxxxx"
SCOPE = ["http://secret.com"]











def initialize(base_address=BASE_ADDRESS, client_id=CLIENT_ID, secret=CLIENT_SECRET):
    try:
        response = requests.get(f"{base_address}/api/v2/AuthenticationAuthority")
        response.raise_for_status()
    except Exception as e:
        print(f"fout in initialize() met url: {e}")
        raise

    authentication_authority = response.text.strip()

    try:
        app = ConfidentialClientApplication(
            client_id,
            authority=authentication_authority,
            client_credential=secret
        )

        result = app.acquire_token_for_client(scopes=SCOPE)

        if "access_token" not in result:
            raise Exception(f"Failed to acquire token: {result.get('error_description')}")
    except Exception as e:
        print(f"fout in initialize() met token: {e}")
        raise

    access_token = result["access_token"]

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {access_token}"
    })
    session.base_url = base_address  

    print("token gehaald")
    return session













def extract_json_from_response(response_text):
    try:
        code_block_match = re.search(r"```json\s*(\{.*?\}|\[.*?\])\s*```", response_text, re.DOTALL)
        if code_block_match:
            json_str = code_block_match.group(1)
        else:
            json_match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", response_text)
            if json_match:
                json_str = json_match.group(1)
            else:
                raise ValueError("No JSON object or array found in the response.")
        return json.loads(json_str)
    except Exception as e:
        print(f"fout in extract_json_from_response(): {e}")
        raise














def safe_json_loads(obj):
    if isinstance(obj, str):
        obj = obj.strip()
        if obj:  
            try:
                return json.loads(obj)
            except json.JSONDecodeError as e:
                print(f"fout met json.loads(): {e} -> json: {repr(obj)}")
                return None
    return obj








def fetch_measuring_points_data(locations, cities, from_date, until_date):
    all_data = []

    if isinstance(locations, str):
        try:
            locations = json.loads(locations)
        except json.JSONDecodeError as e:
            print(f"fout met locations parsen: {e}")
            return []

    json_locations = locations
    session = initialize()


    for city in cities:
        for location in json_locations:
            try:
             
                if isinstance(location, str):
                    try:
                        location = json.loads(location)
                    except json.JSONDecodeError:
                        print(f"slechte location: {location}")
                        continue

                if not isinstance(location, dict):
                    print(f"⚠️ geen dict location: {location}")
                    continue

                address = location.get('address', {})
                city_name = address.get('city', '').strip()

                if city_name != city:
                    continue

                measuring_points = location.get('measuringPoints', [])

                for point in measuring_points:
                    if isinstance(point, str):
                        try:
                            point = json.loads(point)
                        except json.JSONDecodeError:
                            print(f"invalide measuringpoint string: {point}")
                            continue

                    if not isinstance(point, dict):
                        print(f"geen dict: {point}")
                        continue

                    guid = point.get('guid')
                    if not guid:
                        continue

                    url = (
                        f"https://api.signcoserv.be/api/integrations/generic/v1/MeasuringPoints/"
                        f"{guid}/Vehicles?from={from_date}&until={until_date}"
                        f"&includeExceptional=false&includeError=false&includePredicted=true"
                        f"&includeVehicleCategory=true&includeSpeed=false&includeIsReverse=false"
                    )

                    try:
                        response = session.get(url)
                        response.raise_for_status()
                        json_data = response.json()

                        vehicle_data = json_data.get('data', [])

                        all_data.append({
                            'city': city,
                            'guid': guid,
                            'vehicleData': vehicle_data
                        })
                    except Exception as e:
                        print(f"fout ophalen measuring point {guid} voor {city}: {e}")
            except Exception as e:
                print(f"fout ophalen location voor stad {city}: {e}")

    return all_data


















def extract_cities_from_llm_response(llm_response, valid_cities):
    try:
        possible_cities = re.findall(r'\b[A-Za-zÀ-ÿ\-]+(?:\s+[A-Za-zÀ-ÿ\-]+)*\b', llm_response)

        valid_cities_lower = {city.lower(): city for city in valid_cities}
        cleaned_cities = set()

        for city_candidate in possible_cities:
            city_candidate_clean = city_candidate.strip()
            if not city_candidate_clean:
                continue

            match = valid_cities_lower.get(city_candidate_clean.lower())
            if match:
                cleaned_cities.add(match)

        return list(cleaned_cities)
    except Exception as e:
        print(f"fout in extract_cities_from_llm_response(): {e}")
        raise



def extract_dates_from_sentence(sentence):
    date_strings = re.findall(r'\d{4}-\d{2}-\d{2}', sentence)

    dates = []
    for date_str in date_strings:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            dates.append(date_obj)
        except ValueError:
            print(f"geen date format: {date_str}")
            continue

    if not dates:
        print("geen dates gevonden.")
        return []

    dates.sort()

    return [dates[0].strftime("%Y-%m-%d"), dates[-1].strftime("%Y-%m-%d")]




















def get_measuring_points():
    context = ""
    index = 0
    try:
        session = initialize()
        response = session.get(f"{BASE_ADDRESS}/api/integrations/generic/v1/MeasuringPoints?onlyActive=true")

        if response.status_code != 200:
            raise Exception(f"Failed to fetch measuring points: {response.status_code} - {response.text}")

        measuring_points = response.json().get("data", [])

        today = date.today()

 

        while True:
            user_input = input("input: ")
            dates_prompt = f"""
        today is {today.strftime('%Y-%m-%d')}.
        You have to find the start and end date in the user input.
        Here is the user input:
        {user_input}
        for example if the user input is: I want all bikes that drove around in neighbouring cities of Lier between 2023-01-01 and 2023-12-31
        you should return: 2023-01-01, 2023-12-31

        if the user input is: I want all bikes that drove around in neighbouring cities of Lier from last week to today
        you should return: {today - timedelta(days=7)}, {today.strftime('%Y-%m-%d')}
        so just return the start and end date in the format YYYY-MM-DD, separated by a comma.
        Don't say what youre reasong is or any examples and definitely don't say any other dates or anything else.
        Just return the dates in the format YYYY-MM-DD, separated by a comma.
        """
            dates_response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3",
                    "prompt": dates_prompt,
                    "stream": False
                }
            )
            print(dates_response.json()["response"])
            extracted_dates = extract_dates_from_sentence(dates_response.json()["response"])

            while not extracted_dates:
                print("geen dates gevonden, probeer opnieuw:")
                user_input = input("input: ")
                extracted_dates = extract_dates_from_sentence(user_input)


            print(f"hier is de user input: {user_input}")

            cities = [
                "Willebroek", "Berchem", "Hove", "Bornem", "Lier", "Hoogstraten", "Arendonk", "Turnhout",
                "Grobbendonk", "Vlimmeren", "Ramsel", "Kapellen", "Mechelen", "Geel", "Rijkevorsel", "Boechout",
                "Herentals", "Kasterlee", "Antwerpen", "Duffel", "Puurs-Sint-Amands", "Balen", "Lint",
                "Beerse", "Wortel", "Retie", "Boom", "Mortsel"
            ]

            prompt = f"""
Willebroek,Berchem,Hove,Bornem,Lier,Hoogstraten,Arendonk,Turnhout,Grobbendonk,Vlimmeren,Ramsel,Kapellen,Mechelen,Geel,Rijkevorsel,Boechout,Herentals,Kasterlee,Antwerpen,Duffel,Puurs-Sint-Amands,Balen,Lint,Beerse,Wortel,Retie,Boom,Mortsel
These are all cities located in Belgium.

Which cities do you think are needed for this user request? Try to answer shortly and just answer with the names of the cities that you think are needed.

For example if the users ask for all cities which have the letter eye you should return: Berchem, Hove, Bornem, Hoogstraten, Arendonk, Turnhout, Grobbendonk, Vlimmeren, Ramsel, Kapellen, Mechelen, Geel, Rijkevorsel, Boechout, Herentals, Kasterlee, Antwerpen, Duffel,Balen, Beerse, Wortel, Retie, Mortsel.
For example if the user asks for all cities that start with the letter H you should return: Hove, Hoogstraten, Herentals, Hove.
For example if the user asks for all cities that border Lier you should return: Boechout, Lint, Duffer
{user_input}"""

            ollama_response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3",
                    "prompt": prompt,
                    "stream": False
                }
            )
            ollama_response.raise_for_status()
            raw_response = ollama_response.json()["response"]

            print("-------------------------------------------------------------------------------------------------------")
            print("model output:")
            print(raw_response)

            extracted_cities = extract_cities_from_llm_response(raw_response, cities)

            measuring_points_data = fetch_measuring_points_data(measuring_points, extracted_cities, extracted_dates[0], extracted_dates[1])

            print("measuring_points_data:")
            print(measuring_points_data)

            measuring_points_json = json.dumps(measuring_points_data, indent=4)

            prompt2 = f"""
Based on the following data, please provide an answer to this user request in json format:
Here is the history of the previous conversations. You can use this information to answer the user's request.
{context}

Here is the new Question, you have to answer this question only include the data that is asked for and relevant to the question, also don't do this 85 + 41 just give the answer to the sum so in this case 126. Because 85 + 41 is not valid json, it has to be all strings or numbers also when you do a sum of the context always write the answer don't write the sum of the numbers just the answer always numbers or strings:
{user_input}
The data given contains all the information needed to answer the user's request.

Please respond in JSON format.
Here is the new data that is tailormade for the user's new question:
```json
{measuring_points_json}
"""
            print("-------------------------------------------------------------------------------------------------------")

            ollama_response2 = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3",
                    "prompt": prompt2,
                    "stream": False
                }
            )

            json_response_ollama = ollama_response2.json()["response"]

            print(json_response_ollama)
            try:
                json_data = extract_json_from_response(json_response_ollama)
                df = pd.DataFrame.from_dict(json_data, orient='index', columns=['result'])
                df.index.name = 'city'
                df.reset_index(inplace=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"output_{timestamp}.xlsx"
                df.to_excel(filename, index=False)
            except Exception as e:
                print(f"fout met het maken van de excel: {e}")

            context += f"\n\nPrevious user question {index}: {user_input}"
            context += f"\nPrevious model output {index}: ```json\n{measuring_points_json}\n```"
            index += 1

    except Exception as e:
        print(f"fout in get_measuring_points(): {e}")
        return None





def extract_json_from_string(text):
    try:
        match = re.search(r'(\{.*?\}|\[.*?\])', text, re.DOTALL)
        
        if match:
            json_str = match.group(1)
            parsed_json = json.loads(json_str)
            return parsed_json
        else:
            print("geen json.")
            return None

    except json.JSONDecodeError as e:
        print(f"iets gevonden dat lijkt op json maar lukt niet om te parsen: {e}")
        return None

def ultra_flatten_json(data):
    flat_data = []

    for record in data:
        if not isinstance(record, dict):
            continue

        base_info = {}
        vehicle_entries = []

        for key, value in record.items():
            if isinstance(value, list):
                vehicle_entries = value
            else:
                base_info[key] = value

        for entry in vehicle_entries:
            if isinstance(entry, dict):
                combined = dict(base_info)  
                combined.update(entry)      
                flat_data.append(combined)
            else:
                print(f"fout: {entry}")

    return flat_data


get_measuring_points()
