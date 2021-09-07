import pandas as pd
class Trip:
    def __init__(self, normalized_df) -> None:
        self.normalized_df = normalized_df
        self.columns = ['id', 'datetime_pickup_id', 'datetime_dropoff_id', 'fare_id',
                'pickup_location_id','dropoff_location_id', 'payment_id','passenger_count',
                'trip_distance', 'trip_duration']
    
    def create_trips_table(self):
        self.dataframe = pd.DataFrame(columns=self.columns)
        for index, row in self.normalized_df.iterrows():
            new_row = self.parse_data(index)
            self.dataframe = self.dataframe.append(new_row, ignore_index=True)
    
    def parse_data(self, idx):
        new_row = {}
        new_row['id'] = self.normalized_df.at[idx, 'tip_id']
        new_row['datetime_pickup_id'] = self.normalized_df.at[idx, 'datetime_pickup_id']
        new_row['datetime_dropoff_id'] = self.normalized_df.at[idx, 'datetime_dropoff_id']
        new_row['fare_id'] = self.normalized_df.at[idx, 'fare_id']
        new_row['pickup_location_id'] = self.normalized_df.at[idx, 'pickup_location_id']
        new_row['dropoff_location_id'] = self.normalized_df.at[idx, 'dropoff_location_id']
        new_row['payment_id'] = self.normalized_df.at[idx, 'payment_id']
        new_row['passenger_count'] = self.normalized_df.at[idx, 'passenger_count']
        new_row['trip_distance'] = self.normalized_df.at[idx, 'trip_distance']
        new_row['trip_duration'] = self.normalized_df.at[idx, 'trip_duration']
        return new_row

