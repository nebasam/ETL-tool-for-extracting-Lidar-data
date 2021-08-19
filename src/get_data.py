import pdal
import json



PUBLIC_DATA_URL = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/"
REGION = "IA_FullState"
bounds = "([-93.756155, 41.918015], [-93.747334, 41.921429])"
PUBLIC_ACCESS_PATH=f"{PUBLIC_DATA_URL}{REGION}/ept.json"
out_put_laz_path = "./data/laz/Iowa.laz"
out_put_tif_path = "./data/tif/Iowa.tif"
PIPLINE_PATH='./get_data.json'

pipeline_path = './data.json'



def get_raster_terrain(bounds:str , region:str , public_access_path =     PUBLIC_ACCESS_PATH, output_filename_laz=out_put_laz_path,
                       ouput_filename_tif = out_put_tif_path,pipeline_path =pipeline_path )->None:

    with open(pipeline_path) as json_file:
        the_json = json.load(json_file)


    the_json['pipeline'][0]['bounds']=bounds
    the_json['pipeline'][0]['filename']=public_access_path
    the_json['pipeline'][5]['filename']=output_filename_laz
    the_json['pipeline'][6]['filename']=ouput_filename_tif

    pipeline = pdal.Pipeline(json.dumps(the_json))

    try:
        
        exxec = pipeline.execute()
        metadata = pipeline.metadata

    except RuntimeError as e :
        print(e)
        pass


if (__name__== '__main__'):
    get_raster_terrain(bounds=bounds,region=REGION)