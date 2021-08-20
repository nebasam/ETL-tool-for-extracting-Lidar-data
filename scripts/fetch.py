import geopandas as gpd

import pdal
import json
from shapely.geometry import shape, GeometryCollection, Polygon,Point
import os
import sys
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join('..')))
import pyproj

from shapely.ops import transform

class FetchData:
    def __init__(self,region) -> None:
        self.region=region
    def fetch_elevation(self,boundary, crs):
        try:
            pat=Path(__file__).parent.joinpath("fetch_template.json")
            with open(pat, 'r') as json_file:
                pipeline = json.load(json_file)
        except FileNotFoundError as e:
            print('FETCH_JSON_FILE_NOT_FOUND')
        
        boundary_repojected=self.repoject(boundary,crs)
        Xmin,Ymin,Xmax,Ymax=boundary_repojected.bounds
        pipeline[0]["filename"]=f"https://s3-us-west-2.amazonaws.com/usgs-lidar-public/{self.region}/ept.json"
        pipeline[0]["bounds"]=f"([{Xmin},{Xmax}],[{Ymin},{Ymax}])"
        pipeline[1]["polygon"]=str(boundary_repojected)
        pipeline[4]["out_srs"]=crs
        # print( pipeline[0]["polygon"])
        
        pipe = pdal.Pipeline(json.dumps(pipeline))
        count = pipe.execute()
        arrays = pipe.arrays    
        metadata = pipe.metadata
        log = pipe.log
        years=[]
        for i in arrays:
            geometry_points = [Point(x, y) for x, y in zip(i["X"], i["Y"])]
            elevetions = i["Z"]
            frame=gpd.GeoDataFrame(columns=["elevation", "geometry"])
            frame['elevation'] = elevetions
            frame['geometry'] = geometry_points
            frame.set_geometry("geometry",inplace=True)
            frame.set_crs(epsg=crs[-4:] , inplace=True)
            years.append(frame)
        return years
    def repoject(self,polygon,crs):
        
        wgs84 = pyproj.CRS(crs)
        utm = pyproj.CRS('EPSG:3857')

        project = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
        utm_point = transform(project, polygon)
        return utm_point