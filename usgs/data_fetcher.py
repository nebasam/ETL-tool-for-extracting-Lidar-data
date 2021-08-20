import logging
import pdal
from json import load, dumps
import sys
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.geometry import Point
from logger_creator import CreateLogger


logger = CreateLogger('DataFetcher')
logger = logger.get_default_logger()


class DataFetcher():
    def __init__(self, polygon: Polygon, region: str, epsg: str) -> None:
        try:
            self.get_polygon_edges(polygon, epsg)
            self.data_location = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
            self.load_pipeline_template()
            self.region = region
            self.epsg = epsg

            logger.info('Successfully Instantiated DataFetcher Class Object')

        except Exception as e:
            logger.exception('Failed to Instantiate DataFetcher Class Object')
            sys.exit(1)

    def load_pipeline_template(self, file_name='./pipeline_template.json'):
        try:
            with open(file_name, 'r') as read_file:
                template = load(read_file)

            self.template_pipeline = template

            logger.info('Successfully Loaded Pdal Pipeline Template')

        except Exception as e:
            logger.exception('Failed to Load Pdal Pipeline Template')
            sys.exit(1)

    def get_polygon_edges(self, polygon: Polygon, epsg: str):
        try:
            grid = gpd.GeoDataFrame([polygon], columns=["geometry"])
            grid.set_crs(epsg=epsg, inplace=True)

            grid['geometry'] = grid.geometry.to_crs(epsg=3857)

            minx, miny, maxx, maxy = grid.geometry[0].bounds
            # bounds: ([minx, maxx], [miny, maxy])
            self.extraction_bounds = f"({[minx, maxx]},{[miny,maxy]})"

            # Cropping Bounds
            self.polygon_cropping = self.get_crop_polygon(grid.geometry[0])

            grid['geometry'] = grid.geometry.to_crs(epsg=epsg)
            self.geo_df = grid

            logger.info(
                'Successfully Extracted Polygon Edges and Polygon Cropping Bounds')

        except Exception as e:
            logger.exception(
                'Failed to Extract Polygon Edges and Polygon Cropping Bounds')

    def get_crop_polygon(self, polygon: Polygon):
        polygon_cords = 'POLYGON(('
        for i in list(polygon.exterior.coords):
            polygon_cords += f'{i[0]} {i[1]},'

        polygon_cords = polygon_cords[:-1] + '))'

        return polygon_cords

    def construct_simple_pipeline(self):
        self.pipeline = []
        reader = self.template_pipeline['reader']
        reader['bounds'] = self.extraction_bounds
        reader['filename'] = self.data_location + self.region + "/ept.json"
        self.pipeline.append(reader)

        cropper = self.template_pipeline['cropping_filter']
        cropper['polygon'] = self.polygon_cropping
        self.pipeline.append(cropper)

        self.pipeline.append(self.template_pipeline['range_filter'])
        self.pipeline.append(self.template_pipeline['assign_filter'])

        reprojection = self.template_pipeline['reprojection_filter']
        reprojection['out_srs'] = f"EPSG:{self.epsg}"
        self.pipeline.append(reprojection)

        self.pipeline = pdal.Pipeline(dumps(self.pipeline))

    def construct_pipeline_template_1(self, file_name: str, resolution: int = 1, window_size: int = 6, tif_values: list = ["all"]):
        self.pipeline = []
        reader = self.template_pipeline['reader']
        reader['bounds'] = self.extraction_bounds
        reader['filename'] = self.data_location + self.region + "/ept.json"
        self.pipeline.append(reader)

        self.pipeline.append(self.template_pipeline['range_filter'])
        self.pipeline.append(self.template_pipeline['assign_filter'])

        reprojection = self.template_pipeline['reprojection_filter']
        reprojection['out_srs'] = f"EPSG:{self.epsg}"
        self.pipeline.append(reprojection)

        # Simple Morphological Filter
        self.pipeline.append(self.template_pipeline['smr_filter'])
        self.pipeline.append(self.template_pipeline['smr_range_filter'])

        laz_writer = self.template_pipeline['laz_writer']
        laz_writer['filename'] = f"{file_name}_{self.region}.laz"
        self.pipeline.append(laz_writer)

        tif_writer = self.template_pipeline['tif_writer']
        tif_writer['filename'] = f"{file_name}_{self.region}.tif"
        tif_writer['output_type'] = tif_values
        tif_writer["resolution"] = resolution
        tif_writer["window_size"] = window_size
        self.pipeline.append(tif_writer)

        self.pipeline = pdal.Pipeline(dumps(self.pipeline))

    def get_data(self):
        try:
            self.data_count = self.pipeline.execute()

        except Exception as e:

            sys.exit(1)

    def get_pipeline_arrays(self):
        return self.pipeline.arrays

    def get_pipeline_metadata(self):
        return self.pipeline.metadata

    def get_pipeline_log(self):
        return self.pipeline.log

    def get_elevation_geodf(self):
        elevation = gpd.GeoDataFrame()
        elevations = []
        points = []
        for row in self.get_pipeline_arrays()[0]:
            lst = row.tolist()[-3:]
            elevations.append(lst[2])
            point = Point(lst[0], lst[1])
            points.append(point)

        elevation['elevation'] = elevations
        elevation['geometry'] = points
        elevation.set_crs(epsg=self.epsg, inplace=True)

        self.elevation_geodf = elevation

        return self.elevation_geodf


if __name__ == "__main__":
    MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.747334, 41.921429]
    polygon = Polygon(((MINX, MINY), (MINX, MAXY),
                       (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)))

    df = DataFetcher(polygon=polygon, region="IA_FullState", epsg="4326")

    df.construct_simple_pipeline()

    df.get_data()

    elevation = df.get_elevation_geodf()

    print(elevation.sample(10))
