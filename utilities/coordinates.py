def format_coord(lat:str, lon:str) -> tuple:
    lat = lat.replace(",", ".")
    lon =lon.replace(",", ".")
    lat = float(lat)
    lon = float(lon)
    return lon, lat