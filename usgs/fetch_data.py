import pdal
import json
import geopandas as gpd
from shapely.geometry import Polygon, Point
import sys
from logger import Logger
from file_read import FileHandler

logger = Logger()
logger = logger.get_logger('FetchData')

class FetchData:

    def __init__(self, public_data_url = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/", json_path="./get_data.json") -> None:
        try:
            self.logger = Logger().get_logger(__name__)
            self.file_handler = FileHandler()
            self.pipeline_json = self.file_handler.read_json(json_path)
            self.public_data_url = public_data_url
            self.input_epsg = 3857
            

            logger.info('Successfully Instantiated FetchData Class Object')
        except Exception as e:
            logger.exception('Failed to Instantiate FetchData Class Object')
            sys.exit(1)
    def get_polygon_boundaries(self, polygon: Polygon):
        polygon_df = gpd.GeoDataFrame([polygon], columns=['geometry'])

        polygon_df.set_crs(epsg=self.output_epsg, inplace=True)
        polygon_df['geometry'] = polygon_df['geometry'].to_crs(epsg=self.input_epsg)
        minx, miny, maxx, maxy = polygon_df['geometry'][0].bounds

        polygon_input = 'POLYGON(('
        xcords, ycords = polygon_df['geometry'][0].exterior.coords.xy
        for x, y in zip(list(xcords), list(ycords)):
            polygon_input += f'{x} {y}, '
        polygon_input = polygon_input[:-2]
        polygon_input += '))'

        print(polygon_input)
        print(f"({[minx, maxx]},{[miny,maxy]})")

        return f"({[minx, maxx]},{[miny,maxy]})", polygon_input

    def get_pipeline(self, region: str, polygon: Polygon, output_filename: str = "temp"):
        boundaries, polygon_input = self.get_polygon_boundaries(polygon)

        full_dataset_path = f"{self.public_data_url}{region}/ept.json"

        self.pipeline_json['pipeline'][0]['filename'] = full_dataset_path
        self.pipeline_json['pipeline'][0]['bounds'] = boundaries
        self.pipeline_json['pipeline'][1]['polygon'] = polygon_input
        self.pipeline_json['pipeline'][3]['out_srs'] = f'EPSG:{self.output_epsg}'
        self.pipeline_json['pipeline'][4]['filename'] = "../data/laz/" + output_filename + ".laz"
        self.pipeline_json['pipeline'][5]['filename'] = "../data/tif/" + output_filename + ".tif"

        pipeline = pdal.Pipeline(json.dumps(self.pipeline_json))

        return pipeline

    def run_pipeline(self, polygon: Polygon, epsg, region: str = "IA_FullState"):
        self.output_epsg = epsg
        pipeline = self.get_pipeline(region, polygon)

        try:
            pipeline.execute()
            self.logger.info(f'Pipeline executed successfully.')
            return pipeline
        except RuntimeError as e:
            self.logger.exception('Pipeline execution failed')
            print(e)

    def make_geo_df(self, arr):
        geometry_points = [Point(x, y) for x, y in zip(arr["X"], arr["Y"])]
        elevetions = arr["Z"]
        df = gpd.GeoDataFrame(columns=["elevation", "geometry"])
        df['elevation'] = elevetions
        df['geometry'] = geometry_points
        df = df.set_geometry("geometry")
        df.set_crs(self.output_epsg, inplace=True)
        return df

    def get_data(self, polygon: Polygon, epsg):
        pipeline = self.run_pipeline(polygon, epsg)
        arr = pipeline.arrays[0]
        return self.make_geo_df(arr)
if(__name__ == '__main__'):
    MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.756055, 41.918115]
    polygon = Polygon(((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)))
    data_fetcher = FetchData()
    print(data_fetcher.get_data(polygon, epsg=4326))