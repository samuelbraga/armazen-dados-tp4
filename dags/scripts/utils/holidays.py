import requests
from requests import Response
import json
from datetime import datetime
class Holidays:
    def __init__(self, year):
        self.year = year
        self.holidays_dates = self.get_holidays()
    
    def get_holidays(self):
        api = PublicHolydaysApi()
        holidays_dicts = api.get_holidays_for_year(self.year)
        holidays_dates = []
        for holiday_dict in holidays_dicts:
            holiday = datetime.strptime(holiday_dict['date'], '%Y-%m-%d')
            holidays_dates.append(holiday.date())
        return holidays_dates
    
    def is_holiday(self, date: datetime):
        if date.date() in self.holidays_dates:
            return True
        else:
            return False
        

class PublicHolydaysApi:
    def __init__(self, version = 'v3'):
        self.base_url = f'https://date.nager.at/api/{version}/publicholidays'
    
    def get_holidays_for_year(self, year, country_code='US', countie_code='NY'):
        request_url = self.base_url+f'/{year}/{country_code}'
        response = requests.get(request_url)
        if not response.ok:
            raise Exception("Something went wrong with the Holidays API")
        response = response.json()
        if countie_code != None:
            holidays = []
            for holiday in response:
                if holiday['global'] == True:
                    holidays.append(holiday)
                else:
                    if country_code+'-'+countie_code in holiday['counties']:
                        holidays.append(holiday)
        else:
            holidays = response
        return holidays
