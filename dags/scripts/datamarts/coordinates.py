import pandas as pd
class Coordinates:
    def __init__(self, normalized_df: pd.DataFrame):
        self.normalized_df = normalized_df
        self.coordinates_columns = ['id', 'latitude', 'longitude']
    
    def create_coordinates_table(self):
        self.dataframe = pd.DataFrame(columns=self.coordinates_columns)
        for index, row in self.normalized_df.iterrows():
            self.extract_pickup_coordinates(index)
            self.extract_dropoff_coordinates(index)
    
    def extract_dropoff_coordinates(self, idx):
        new_row = self.extract_coordinates(idx, 'dropoff_')
        self.dataframe = self.dataframe.append(new_row, ignore_index=True)

    def extract_pickup_coordinates(self, idx):
        new_row = self.extract_coordinates(idx, 'pickup_')
        self.dataframe = self.dataframe.append(new_row, ignore_index=True)

        
    def extract_coordinates(self, idx, prefix):
        coord_row = {}
        coord_row['longitude'] = self.normalized_df.at[idx, prefix+'longitude']
        coord_row['latitude'] = self.normalized_df.at[idx, prefix+'latitude']
        coord_row['id'] = self.normalized_df.at[idx, prefix+'location_id']
        return coord_row
