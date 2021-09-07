import requests
class Location:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def parse_dimension(self):
        self.create_columns()
        self.dataframe = self.dataframe.apply(lambda x: PandasParser(x).get_locations(), axis=1)
        
    
    def create_columns(self):
        self.dataframe["pick_up_city"] = ""
        self.dataframe["pick_up_locality"] = ""
        self.dataframe["dropoff_city"] = ""
        self.dataframe["dropoff_locality"] = ""
    
        
class PandasParser:
    def __init__(self, row):
        self.row = row
    
    def get_locations(self):
        self.get_pick_up_location()
        self.get_dropoff_location()
        return self.row
    
    def get_pick_up_location(self):
        pick_up_location = ReverseGeoCodeApi.get_location(self.row['pickup_latitude'],self.row['pickup_longitude'])
        self.row['pick_up_city'] = pick_up_location['city']
        self.row['pick_up_locality'] = pick_up_location['locality']
    
    def get_dropoff_location(self):
        dropoff_location = ReverseGeoCodeApi.get_location(self.row['pickup_latitude'],self.row['pickup_longitude'])
        self.row['dropoff_city'] = dropoff_location['city']
        self.row['dropoff_locality'] = dropoff_location['locality']
        

class ReverseGeoCodeApi:
    @staticmethod
    def get_address(latitude, longitude):
        base_url = 'https://api.bigdatacloud.net/data/reverse-geocode-client'
        request_fields = {
            'latitude': latitude,
            'longitude': longitude,
            'localityLanguage': 'en'
        }
        response = requests.get(base_url, request_fields)
        response = response.json()
        return response
    @staticmethod
    def get_location(latitude, longitude):
        data =  ReverseGeoCodeApi.get_address(latitude, longitude)
        location = {'city': data['city'], 'locality':data['locality']}
        return location
