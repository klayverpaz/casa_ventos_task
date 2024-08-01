import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

class DataService:

    DATA_URL = 'https://sigel.aneel.gov.br/arcgis/rest/services/PORTAL/WFS/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=pjson'

    @staticmethod
    def get_sigel_data() -> dict:
        try:
            response = requests.get(DataService.DATA_URL)
            response.raise_for_status()
            data = response.json() 
            return data
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None
        
    @staticmethod
    def export_gdf_to_csv(gdf, filename):
        # Drop the geometry column as it's not needed in the CSV
        df = gdf.drop(columns='geometry')

        # Ensure the directory exists
        output_dir = os.path.dirname(filename)
        os.makedirs(output_dir, exist_ok=True)

        # Export the DataFrame to a CSV file
        df.to_csv(filename, index=False)


    @staticmethod
    def get_aerogenerators_preprocessed_df() -> pd.DataFrame:
        json_data = DataService.get_sigel_data()

        features = json_data['features']

        attributes = [feature['attributes'] for feature in features]
        df = pd.DataFrame(attributes)
        df['DATA_ATUALIZACAO'] = pd.to_datetime(df['DATA_ATUALIZACAO'], unit='ms')

        geometry = [Point(feature['geometry']['x'], feature['geometry']['y']) for feature in features]

        gdf = gpd.GeoDataFrame(df, geometry=geometry)

        ## Suponha que o sistema UTM seja 23S
        ## https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.set_crs.html
        ## EPSG:4674 => SIRGAS 2000 datum => https://epsg.io/4674
        crs = f"EPSG:{json_data['spatialReference']['wkid']}"
        gdf.set_crs(crs, inplace=True)

        # Add latitude and longitude columns
        gdf['latitude'] = gdf.geometry.y
        gdf['longitude'] = gdf.geometry.x

        return gdf