import csv
import mapbox_vector_tile
from pyproj import Transformer
from shapely.geometry import Point

mvt_extent = 4096.0
web_mercator_extent = 20037508.342789244
scale_factor = mvt_extent / web_mercator_extent

SRID_LNGLAT = 4326
SRID_SPHERICAL_MERCATOR = 3857
direct_transformer = Transformer.from_crs(crs_from=SRID_LNGLAT, crs_to=SRID_SPHERICAL_MERCATOR, always_xy=True)


def write_mvt_content(trees, path, file_name):
    file_path = f"{path}{file_name}.mvt"
    features = to_features(trees)
    tile_pbf = mapbox_vector_tile.encode(
        {
            "name": "trees",
            "features": features
        }, default_options={"quantize_bounds": (12.0, 51.0, 13.0, 52.0)})
    with open(file_path, "wb") as binary_file:
        binary_file.write(tile_pbf)


def to_features(trees):
    features = []
    for tree in trees:
        # id, lng, lat, radolan_sum, age
        point = Point(tree[1],tree[2])
        if tree[3].isdigit():
            radolan_sum = int(tree[3])
        else:
            radolan_sum = None
        if tree[4].isdigit():
            age = int(tree[4])
        else:
            age = None
        features.append({
            "geometry": point.wkt,
            "properties": {
                "id": tree[0],
                "radolan_sum": radolan_sum,
                "age": age
            }
        })
    print(features[0])
    return features


def write_radolan_mvts(path):
    file_name = "trees"
    file_path = f"{path}{file_name}.csv"
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(csv_reader)
        trees = [row for row in csv_reader]
        write_mvt_content(trees, path, file_name)
