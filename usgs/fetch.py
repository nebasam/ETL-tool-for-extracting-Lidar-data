from get_data import Lidar_Data_Fetch
from boundaries import Boundaries


REGION = "IA_FullState"
PUBLIC_DATA_URL = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
if __name__ == "__main__":
    fetcher = Lidar_Data_Fetch(PUBLIC_DATA_URL)

    xmin, ymin = -93.756155, 41.918015
    xmax, ymax = -93.747334, 41.921429
    bounds = Boundaries(ymin, xmin, ymax, xmax)

    fetcher.runPipeline(REGION, bounds)