import sys, os

sys.path.append(os.path.abspath(os.path.join('../scripts')))
import importlib
from scripts.fetch import FetchData
# importlib.reload(fetch)
from shapely.geometry import shape, GeometryCollection, Polygon
import pyproj
MINX, MINY, MAXX, MAXY = [-93.756155, 41.918015, -93.747334, 41.921429]
polygon = Polygon(((MINX, MINY), (MINX, MAXY), (MAXX, MAXY), (MAXX, MINY), (MINX, MINY)))

FetchData("IA_FullState").fetch_elevation(polygon,"EPSG:4326")