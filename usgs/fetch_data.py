import pdal
import json
import geopandas as gpd
from shapely.geometry import Polygon, Point
import sys
from logger import Logger
from file_read import FileHandler
import numpy as np
import matplotlib.pyplot as plt

class FetchData():
    """A Data Fetching Class which handles fetching, loading, 
        transforming, and visualization from AWS dataset 

        Parameters
        ----------
        polygon : Polygon
        Polygon of the area which is being searched for
        epsg : str
        CRS system which the polygon is constructed based on
        region: str, optional
        Region where the specified polygon is located in from the file name folder located in the AWS dataset. If
        not provided the program will search and provide the region if it is in the AWS dataset

        Returns
        -------
        None
        """    
    def __init__(self, polygon: Polygon, region: str, epsg: str) -> None:
        try:
            self.logger = Logger().get_logger(__name__)
            self.json_path = "../usgs/get_data.json"
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

    def get_polygon_margin(self, polygon: Polygon, epsg: str) -> tuple:
        """To extract polygon margin and assign polygon input.

        Parameters
        ----------
        polygon : Polygon
            Polygon object describing the boundary of the location required
        epsg : str
            CRS system on which the polygon is constructed on

        Returns
        -------
        tuple
            Returns bounds of the polygon provided(minx, miny, maxx, maxy) and polygon input
        """
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
    def get_pipeline(self, polygon: Polygon, epsg: str, output_filename: str = "farm_land_IA_FullState"):
        """Generates a Pdal Pipeline .

        Parameters
        ----------
        file_name : str
            File name used when saving the tiff and LAZ file
        polygon 
        epsg

        Returns
        -------
        pipeline
        """

        try:
            with open(self.json_path) as json_file:
               self.pipeline_json = json.load(json_file) 
            extraction_boundaries, polygon_input = self.get_polygon_margin(polygon, epsg)
            full_dataset_path = f"{self.public_data_url}{self.region}/ept.json"
            self.pipeline_json['pipeline'][0]['filename'] = full_dataset_path
            self.pipeline_json['pipeline'][0]['bounds'] = extraction_boundaries
            self.pipeline_json['pipeline'][1]['polygon'] = polygon_input
            self.pipeline_json['pipeline'][3]['out_srs'] = f'EPSG:{self.epsg}'
            self.pipeline_json['pipeline'][4]['filename'] = "../data/laz/" + output_filename + ".laz"
            self.pipeline_json['pipeline'][5]['filename'] = "../data/tif/" + output_filename + ".tif"
            pipeline = pdal.Pipeline(json.dumps(self.pipeline_json))
            self.logger.info(f'extracting pipeline successfull.')
            print(pipeline)
            return pipeline  
                
        except RuntimeError as e:
            self.logger.exception('Pipeline extraction failed')
            print(e)
            
        
    def execute_pipeline(self):
        """executes a pdal pipeline
        Parameters
        ----------
        None

        Returns
        -------
        executed pipeline
        """
        pipeline = self.get_pipeline()
        
        try: 
            pipeline.execute()
            self.logger.info(f'Pipeline executed successfully.')
            return pipeline
        except RuntimeError as e:
            self.logger.exception('Pipeline execution failed')
            print(e)

    def make_geo_df(self):
        """Calculates and returns a geopandas elevation dataframe from the cloud points generated before.

        Parameters
        ----------
        None

        Returns
        -------
        gpd.GeoDataFrame with Elevation and coordinate points referenced as Geometry points
        """
        try:
            cloud_points = []
            elevations =[]
            geometry_points=[]
            for row in self.get_pipeline_arrays()[0]:
                lst = row.tolist()[-3:]
                cloud_points.append(lst)
                cloud_points = np.array(cloud_points)
                self.cloud_points = cloud_points
                elevations.append(lst[2])
                point = Point(lst[0], lst[1])
                geometry_points.append(point)
            geodf = gpd.GeoDataFrame(columns=["elevation", "geometry"])
            geodf['elevation'] = elevations
            geodf['geometry'] = geometry_points
            geodf = geodf.set_geometry("geometry")
            geodf.set_crs(epsg = self.epsg, inplace=True)
            self.logger.info(f'extracts geo dataframe')
            return geodf
        except RuntimeError as e:
            self.logger.exception('fails to extract geo data frame')
            print(e)
    def scatter_plot(self, factor_value: int = 1, view_angle: tuple[int, int] = (0, 0)) -> plt:
        """Constructs a scatter plot graph of the cloud points.

        Parameters
        ----------
        factor_value : int, optional
            Factoring value if the data points are huge
        view_angle : tuple(int, int), optional
            Values to change the view angle of the 3D projection

        Returns
        -------
        plt
            Returns a scatter plot grpah of the cloud points
        """

        values = self.cloud_points[::factor_value]

        fig = plt.figure(figsize=(10, 15))

        ax = plt.axes(projection='3d')

        ax.scatter3D(values[:, 0], values[:, 1],
                     values[:, 2], c=values[:, 2], s=0.1, cmap='terrain')

        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_zlabel('Elevation')

        ax.set_title('Scatter Plot of elevation')

        ax.view_init(view_angle[0], view_angle[1])

        return plt
    def terrain_map(self, markersize: int = 10, fig_size: tuple[int, int] = (15, 20)) -> plt:
        """Constructs a Terrain Map from the cloud points.

        Parameters
        ----------
        markersize : int, optional
            Marker size used when ploting the figure
        fig_size : Tuple[int, int], optional
            Size of the figure to be returned

        Returns
        -------
        plt
            Returns a Terrain Map constructed from the cloud points
        """
        self.make_geo_df()

        self.make_geo_df.plot(c='elevation', scheme="quantiles", cmap='terrain', legend=True,
                                  markersize=markersize,
                                  figsize=(fig_size[0], fig_size[1]),
                                  missing_kwds={
                                    "color": "lightgrey",
                                    "edgecolor": "red",
                                    "hatch": "///",
                                    "label": "Missing values"}
                                  )

        plt.title('Terrain Elevation Map')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')

        return plt
    def get_pipeline_arrays(self):
        """Returns the Pdal pipelines retrieved data arrays after the pipeline is run.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        return self.pipeline.arrays    
    
    def get_pipeline_metadata(self):
        """Returns the metadata of Pdal pipelines.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        return self.pipeline.metadata
     
    def get_data(self):
        """Retrieves Data from the Dataset, 

        Parameters
        ----------
        None

        Returns
        -------
        geo dataframe with elevation and geometry point
        """
        self.pipeline = self.execute_pipeline()
        return self.make_geo_df()
if(__name__ == '__main__'):
    MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.756055, 41.918115]
    polygon = Polygon(((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)))
    Fetch_data = FetchData(polygon=polygon, region="IA_FullState", epsg="4326") 
    print(Fetch_data.get_data())