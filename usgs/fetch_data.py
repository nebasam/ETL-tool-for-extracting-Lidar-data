import pdal
import json
import geopandas as gpd
from json import load, dumps
from shapely.geometry import Polygon, Point
import sys
from logger import Logger
from file_read import FileHandler

class FetchData():
    def __init__(self, polygon: Polygon, region: str, epsg: str) -> None:
        try:
            self.logger = Logger().get_logger(__name__)
            self.json_path = "./get_data.json"
            self.file_handler = FileHandler()
            self.pipeline_json = self.file_handler.read_json(self.json_path)
            self.get_polygon_margin(polygon, epsg)
            self.public_data_url = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
            self.region = region
            self.epsg = epsg

            self.logger.info('Successfully Instantiated DataFetcher Class Object')

        except Exception as e:
            self.logger.exception('Failed to Instantiate DataFetcher Class Object')
            sys.exit(1)

    def get_polygon_margin(self, polygon: Polygon, epsg: str):
        try:
            gd_df = gpd.GeoDataFrame([polygon], columns=['geometry'])
            gd_df.set_crs(epsg=epsg, inplace=True)
            gd_df['geometry'] = gd_df['geometry'].to_crs(epsg=3857)
            minx, miny, maxx, maxy = gd_df['geometry'][0].bounds

            polygon_input = 'POLYGON(('
            xcords, ycords = gd_df['geometry'][0].exterior.coords.xy
            for x, y in zip(list(xcords), list(ycords)):
                polygon_input += f'{x} {y}, '
            polygon_input = polygon_input[:-2]
            polygon_input += '))'
            extraction_boundaries = f"({[minx, maxx]},{[miny,maxy]})"
            print(polygon_input)
            print(extraction_boundaries)
            self.logger.info( 'Successfully Extracted Polygon margin and Polygon Input')
            return extraction_boundaries, polygon_input
            
        except Exception as e:
            self.logger.exception(
                'Failed to Extract Polygon margin and Polygon Input')
    def get_pipeline(self, output_filename: str = "temp"):

        try:
            with open(self.json_path) as json_file:
               self.pipeline_json = json.load(json_file) 
            extraction_boundaries, polygon_input = self.get_polygon_margin(polygon,self.epsg)
            full_dataset_path = f"{self.public_data_url}{self.region}/ept.json"
            self.pipeline_json['pipeline'][0]['filename'] = full_dataset_path
            self.pipeline_json['pipeline'][0]['bounds'] = extraction_boundaries
            self.pipeline_json['pipeline'][1]['polygon'] = polygon_input
            self.pipeline_json['pipeline'][3]['out_srs'] = f'EPSG:{self.epsg}'
            self.pipeline_json['pipeline'][4]['filename'] = output_filename + ".laz"
            self.pipeline_json['pipeline'][5]['filename'] = output_filename + ".tif"

            pipeline = pdal.Pipeline(json.dumps(self.pipeline_json))
            self.logger.info(f'extracting pipeline successfull.')
            print(pipeline)
            return pipeline  
                
        except RuntimeError as e:
            self.logger.exception('Pipeline extraction failed')
            print(e)
            
        
    def execute_pipeline(self):
        pipeline = self.get_pipeline()
        
        try: 
            pipeline.execute()
            self.logger.info(f'Pipeline executed successfully.')
            return self.pipeline
        except RuntimeError as e:
            self.logger.exception('Pipeline execution failed')
            print(e)

    def make_geo_df(self, arr):
        geometry_points = [Point(x, y) for x, y in zip(arr["X"], arr["Y"])]
        elevations = arr["Z"]
        df = gpd.GeoDataFrame(columns=["elevation", "geometry"])
        df['elevation'] = elevations
        df['geometry'] = geometry_points
        df = df.set_geometry("geometry")
        df.set_crs(self.output_epsg, inplace=True)
        return df

    def get_data(self):
        self.pipeline = self.execute_pipeline()
        arr = self.pipeline.arrays
        return self.make_geo_df(arr)
if(__name__ == '__main__'):
    MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.756055, 41.918115]
    polygon = Polygon(((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)))
    Fetch_data = FetchData(polygon=polygon, region="IA_FullState", epsg="4326") 
    print(Fetch_data.get_data())