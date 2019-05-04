import re
import json
import time

import numpy
from asammdf import MDF, Signal


def file_selector():
    from glob import glob
    types = ('*.MF4', '*.MDF')
    path = 'data/'
    pathnames = list()
    for type in types:
        pathnames.extend(glob(path + type))
    return pathnames


def file_version(pathname):
    with MDF(pathname) as mdf_file:
        # print('\nMDF Version: %s' % mdf_file.version)
        return mdf_file.version


def read_config(config_path):
    try:
        with open(config_path) as config_file:
            cfg = json.load(config_file)
            # print('Config file [%s] loaded.' % config_path)
    except FileNotFoundError:
        print('Config file [%s] does not exist' % config_path)
        return
    return cfg


def write_config(pathname):
    print('\nGenerating a default signal configuration file...')
    path = re.search(r"[\\/]*.*[\\/]", pathname).group(0)
    # filename = re.search(r"[\\/]*.*[\\/](.*)", pathname).group(1)
    table_name = re.search(r"[\\/]*.*[\\/](.*)\.*\.", pathname).group(1)
    config_name = 'config_' + table_name + '.json'
    config_path = path + config_name
    config = dict()
    with MDF(pathname) as mdf_file:
        counter = 0
        suffix = 1
        for group in mdf_file.groups:
            for channel in group.channels:
                if channel.name != 't':  # May need adaption for other MDF files
                    if channel.name in config:
                        channel.name += '_' + str(suffix)
                        suffix += 1
                    config[channel.name] = channel.name
                    counter += 1
        print('Total Number of Signals: %s' % counter)
    with open(config_path, 'w') as fp_config:
        json.dump(config, fp_config, sort_keys=False, indent=4, ensure_ascii=False)
        print('Signal configuration file [%s] generated.' % config_name)
    return counter


def read_mdf_data(filename, cfg_signals=None, sample_rate=None):
    with MDF(filename) as mdf0_file:
        print('\nStarting to read MDF data...')
        # Number of all channels in MDF
        # n_channels = sum(len(group.channels) for group in mdf0_file.groups)

        # If configuration is given, use only selected channels
        # cfg_signals is a dict where keys = std ch names; vals = raw ch names
        # len(cfg_signals) can be less than or equal to the total number of channels
        if cfg_signals is not None:
            sel_channel_std = list(cfg_signals.keys())
            sel_channel_names = list(cfg_signals.values())
            mdf0_file = mdf0_file.filter(sel_channel_names)

            # Rename signals to standard names
            for i, group in enumerate(mdf0_file.groups):
                for j, channel in enumerate(group.channels):
                    try:
                        id = sel_channel_names.index(channel.name)
                        channel.name = sel_channel_std[id]
                    except:
                        continue
            names = mdf0_file.channels_db
            for i, std_name in enumerate(sel_channel_std):
                names[std_name] = names[sel_channel_names[i]]
                if std_name != sel_channel_names[i]:
                    del names[sel_channel_names[i]]
                print('Signal [%s] renamed to [%s].' % (sel_channel_names[i], std_name))

        # Handle duplicated signal names
        names = list(mdf0_file.channels_db.keys())
        indexes = list(mdf0_file.channels_db.values())
        for i, index in enumerate(indexes):
            # May need adaptation to other types of MDF
            if (len(index) > 1) and (names[i] != 't' and names[i] != 'time'):
                old_name = names[i]
                for j in range(1, len(index)):
                    new_name = old_name + '_' + str(j)
                    new_name_index = mdf0_file.channels_db[old_name][j]
                    gp, ch = new_name_index
                    mdf0_file.channels_db[new_name] = [new_name_index]
                    mdf0_file.groups[gp].channels[ch].name = new_name
                    mdf0_file.channels_db[old_name] = [index[0]]

        # Construct a list of 'data_block_addr' for attachment
        att_addr = list()
        for att in mdf0_file.attachments:
            att_addr.append(att.address)

        # Getting numbers of samples for each channel
        n_samples = list()
        channel_indexes = list()
        channel_names = list()
        for i, group in enumerate(mdf0_file.groups):
            for j, channel in enumerate(group.channels):
                n_samples_current = group.channel_group.cycles_nr
                if channel.name == 't':  # May need adaptation for other MDF files
                    continue
                elif n_samples_current == 0:
                    print('Signal [%s] skipped - empty.' % channel.name)
                    continue
                elif channel.data_block_addr in att_addr:  # channels with attachment will be later discarded
                    print('Signal [%s] skipped - has attachment' % channel.name)
                    continue
                else:
                    n_samples.append(n_samples_current)
                    channel_indexes.append((i, j))
                    channel_names.append(channel.name)
        # print('Number of all channels: %s' % n_channels)
        # print('Number of non-zero channels: %s' % len(n_samples))
        # print('Number of different samples: %s' % len(set(n_samples)))

        # This block is currently not used
        # Non-zero minimum amount of samples
        # indices = numpy.where(n_samples == numpy.min(n_samples[numpy.nonzero(n_samples)]))

        # Looking for channels with maximum number of samples
        indices = numpy.where(n_samples == numpy.max(n_samples))
        # indices[0][0] is timestamp, indices[0][1] is actual channel
        # channel_index in format: (group index, channel index) tuple
        channel_index = channel_indexes[indices[0][1]]
        # Select one channel that has the most samples hence longest timestamps
        longest_channel_name = mdf0_file.groups[channel_index[0]].channels[channel_index[1]].name

        # Extracting only non-zero channels from given MDF
        mdf1_filter = mdf0_file.filter(channel_names)
        mdf1_filter.configure(integer_interpolation=0)
        raster_name = longest_channel_name

        # Re-sample method: based on one channel name or a given sample rate
        if sample_rate:
            mdf2_resample = mdf1_filter.resample(sample_rate)
        else:
            mdf2_resample = mdf1_filter.resample(raster_name)

        # Verification
        # n_channels = sum(len(group.channels) for group in mdf2_resample.groups)
        # n_samples = list()
        # for group in mdf2_resample.groups:
        #     for channel in group.channels:
        #         n_samples.append(group.channel_group.cycles_nr)
        # print('Number of all channels: %s' % n_channels)
        # print('Number of non-zero channels: %s' % len(n_samples))
        # print('Number of different samples: %s' % len(set(n_samples)))

        # Working on channels
        signals = mdf2_resample.select(channel_names, raw=True, dataframe=False, copy_master=True)
        # Convert non-text signals to physical values
        # Generate a list of Postgres data types
        # Construct data block
        sql_data_type = list()  # To be returned
        data_block = numpy.empty((len(channel_names)+1, len(signals[0].timestamps)))
        data_block[0] = signals[0].timestamps
        for i, sig in enumerate(signals):
            # print(sig.timestamps[0], '\t', sig.timestamps[numpy.size(sig.samples)-1], '\t', numpy.size(sig.samples))
            if sig.conversion and sig.conversion.conversion_type < 7:
                signals[i] = sig.physical()
            data_type = str(type(signals[i].samples[0]))
            sql_data_type.append(db_data_type(data_type))  # To be returned
            data_block[i+1] = signals[i].samples

        # Concatenate one time axis and all signal samples
        # Method 1 - take rows, transpose in the end
        # Method 2 - concatenate each row as column
        data_block = numpy.transpose(data_block)  # To be returned
        # Construct data block title series
        sql_data_type.insert(0, 'NUMERIC(8, 3)')  # First column data type for timestamps
        channel_names.insert(0, 'TS')
        data_block_titles = channel_names
        print('Finished reading MDF data.')

    return data_block_titles, data_block, sql_data_type


def db_data_type(data_type):
    # Input is string of data type
    conversion_table = {
        "<class 'numpy.uint8'>":    'int2',     # 0 to 255          # -32768 to 32767
        "<class 'numpy.uint16'>":   'int4',     # 0 to 65535        # -2147483648 to 2147483647
        "<class 'numpy.uint32'>":   'int8',     # 0 to 4924967295   # -9223372036854775808 to 9223372036854775807
        "<class 'numpy.int8'>":     'int2',     # -128 to 127
        "<class 'numpy.int16'>":    'int2',     # -32768 to 32767
        "<class 'numpy.int32'>":    'int4',     # -2147483648 to 2147483647
        "<class 'numpy.float64'>":  'float8',   # double
        "<class 'numpy.bytes_'>":   'text',
    }
    return conversion_table[data_type]


def gis_get_cord(pathname, sample_rate=0.01):
    path = re.search(r"[\\/]*.*[\\/]", pathname).group(0)
    # filename = re.search(r"[\\/]*.*[\\/](.*)", pathname).group(1)
    table_name = re.search(r"[\\/]*.*[\\/](.*)\.*\.", pathname).group(1)
    config_path = path + 'config_' + table_name + '.json'
    try:
        cfg = read_config(config_path)
        # lat lon signals are hardcoded and will be looked for in config files
        sig_name_lat = cfg['GPS_Lat']
        sig_name_lon = cfg['GPS_Lon']
    except Exception:
        print('Signal configuration does not exist. Aborted.')
        return
    with MDF(pathname) as mdf0_file:
        mdf1_filter = mdf0_file.filter([sig_name_lat, sig_name_lon])
        if sample_rate:
            mdf2_resample = mdf1_filter.resample(raster=sample_rate)
            signals = mdf2_resample.select([sig_name_lat, sig_name_lon])
        else:
            signals = mdf1_filter.select([sig_name_lat, sig_name_lon])
        lat = signals[0].samples
        lon = signals[1].samples
        # gps_points = numpy.array([lng_value, lat_value]).transpose()  # for GeoJSON: [longitude, latitude]
        gps_cords = list(zip(lon, lat))
        return gps_cords


def gis_export_geojson(pathname):
    # pathname is the path to the MDF file
    # geojson file will be written in the same folder
    import geojson
    gps_cords = gis_get_cord(pathname)
    path = re.search(r"[\\/]*.*[\\/]", pathname).group(0)
    core_name = re.search(r"[\\/]*.*[\\/](.*)\.*\.", pathname).group(1)
    geojson_name = core_name + '.geojson'
    geojson_path = path + geojson_name
    track = geojson.LineString(gps_cords)
    features = list()
    features.append(geojson.Feature(geometry=track, properties={"Track Name": "Sample Track"}))
    feature_collection = geojson.FeatureCollection(features)
    with open(geojson_path, mode='w') as output:
        geojson.dump(feature_collection, output, indent=0)
    print('Exported file [%s].' % geojson_path)
    return


def gis_map_init(gps_cords, bound_factor=1.15):
    import math
    # gps_cords is a list of (longitude, latitude) tuples
    # OSM Zoom Levels Information:
    # https://wiki.openstreetmap.org/wiki/Zoom_levels
    lon, lat = numpy.transpose(gps_cords)
    lat_n = numpy.max(lat)
    lat_s = numpy.min(lat)
    lon_e = numpy.max(lon)
    lon_w = numpy.min(lon)
    lon_center = (lon_e + lon_w) / 2  # function return
    lat_center = (lat_n + lat_s) / 2  # function return

    # Haversine formula:
    # https://en.wikipedia.org/wiki/Haversine_formula
    # https://www.movable-type.co.uk/scripts/latlong.html
    R = 6371  # mean radius of earth in km
    phi1 = math.radians(lat_s)
    phi2 = math.radians(lat_n)
    phi = math.radians(lat_n - lat_s)
    delta = math.radians(lon_e - lon_w)
    a = math.sin(phi/2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta/2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    great_circle_distance = R * c

    # Minimum map tile size 256 x 256 pixels
    zoom = math.floor(8 - math.log(bound_factor * great_circle_distance / math.sqrt(2 * 256 * 256))) / math.log(2)
    # Convert zoom level from float to integer
    zoom = int(zoom)
    return lon_center, lat_center, zoom


def main():
    t_start = time.time()
    pathnames = file_selector()
    for p in pathnames:
        # file_version(p)
        # cfg = read_config(p)
        # read_mdf_data(p, use_cfg=1)
        # read_mdf_data(p)
        # write_config(p)
        gis_export_geojson(p)
        # gps_cords = gis_get_cord(p)
        # gis_map_init(gps_cords)
    print('\n\n=== Executed in %.3f seconds ===' % (time.time() - t_start))
    return


if __name__ == "__main__":
    main()
