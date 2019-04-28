# import pandas
import numpy
import json
from asammdf import MDF, Signal
from prototype import (
    TEST_FILES,
    SLOW_SIGNALS,
    SIGNALS,
)
from collections import namedtuple


def read_config(config):
    with open(config) as config_file:
        cfg = json.load(config_file)
    return cfg


def file_selector():
    filename = TEST_FILES[0]
    print('\nInput Filename: %s' % filename)
    return filename


def file_version(filename):
    with MDF(filename) as mdf_file:
        print('\nMDF Version: %s' % mdf_file.version)
        return mdf_file.version


def list_all_signals(filename):
    with MDF(filename) as mdf_file:
        counter = 0
        for channel in mdf_file.iter_channels():
            # print(channel.name + '\t[' + channel.unit + ']' + '\t' + channel.source[1])
            print(str(counter) + '\t' + channel.name)
            counter += 1
        print('\nTotal Number of Signals: ' + str(counter) + '\n')
    return


def get_signal_index(signalname, filename):
    with MDF(filename) as mdf_file:
        counter = 0
        for channel in mdf_file.iter_channels():
            if channel.name == signalname:
                return counter
            else:
                counter += 1
        return -1  # Error: signalname not found in MDF file


def signal_values(signals, filename):
    with MDF(filename) as mdf_file:
        channels = mdf_file.select(signals, dataframe=False)
        # TODO: Research pandas dataframe method
        return channels


def data_scrub(filename, slow_signals, fast_signals, slow_freq=1, fast_freq=0.01):
    with MDF(filename) as mdf_file:
        mdf_reduce = mdf_file.filter(fast_signals)
        mdf_fast = mdf_reduce.resample(fast_freq).select(fast_signals, raw=False, dataframe=False)
        mdf_slow = mdf_reduce.resample(slow_freq).select(slow_signals, raw=False, dataframe=False)
        # Construct GPS coordinates
        lat_index = slow_signals.index('GPS_Lat')
        lng_index = slow_signals.index('GPS_Lon')
        speed_index = slow_signals.index('GPS_Velocity')
        lat_value = mdf_slow[lat_index].samples
        lng_value = mdf_slow[lng_index].samples
        # gps_points = numpy.array([lng_value, lat_value]).transpose()  # for GeoJSON: [longitude, latitude]
        gps_points = list(zip(lng_value, lat_value))
        gps_time = numpy.array(mdf_slow[speed_index].timestamps)
        lat1 = numpy.min(mdf_slow[lat_index].samples)
        lat2 = numpy.max(mdf_slow[lat_index].samples)
        lng1 = numpy.min(mdf_slow[lng_index].samples)
        lng2 = numpy.max(mdf_slow[lng_index].samples)
        clat, clng, zoom = map_init(lat2, lat1, lng2, lng1, 1.05)
        # gps_boundaries = numpy.array([bound_s, bound_n, bound_w, bound_e, lat_c, lng_c])
        Boundaries = namedtuple('Boundaries', ['clat', 'clng', 'zoom'])
        map_settings = Boundaries(clat=clat, clng=clng, zoom=zoom)
        return gps_time, gps_points, map_settings


def data_prep(filename, cfg, freq=0.01):
    signals_normal = list(cfg.keys())
    signals_raw = list(cfg.values())
    with MDF(filename) as mdf_file:
        mdf_reduce = mdf_file.filter(signals_raw)
        mdf_output = mdf_reduce.resample(raster=freq)
        # Rename <Raw Signal Names> to <Normalized Signal Names>
        # counter = 0
        # for group in mdf_output.groups:
        #     for channel in group.channels[1::2]:  # Odd channels are "t"; Even channels are signals
        #         # print(counter, channel.name, signals_normal[counter])
        #         channel.name = signals_normal[counter]
        #         counter += 1
        signals = mdf_output.select(signals_raw, raw=False, dataframe=False)
        sql_dtype = data_type(signals)
        # Generate timestamps
        signal_t = signals[0].timestamps
        return signals, sql_dtype, signals_normal, signals_raw, signal_t


def data_type(signals):
    conv = {
        "<class 'numpy.uint8'>":      'int',
        "<class 'numpy.uint16'>":     'int',
        "<class 'numpy.uint32'>":     'int',
        "<class 'numpy.float64'>":    'double',
        "<class 'numpy.bytes_'>":     'text',
    }
    sql_dtype = list()
    for signal in signals:
        sig_dtype = str(type(signal.samples[0]))
        sql_dtype.append(conv[sig_dtype])
        # print(sig_dtype, conv[sig_dtype])
    return sql_dtype


def map_init(lat_s, lat_n, lng_w, lng_e, bound_factor):
    import math
    clat = (lat_n + lat_s) / 2
    clng = (lng_e + lng_w) / 2
    dlat = math.radians(lat_n - lat_s)
    dlng = math.radians(lng_e - lng_w)
    # Haversine formula: calculate the great-circle distance between two points
    a = math.sin(dlat / 2) ** 2 + math.cos(lat_s) * math.cos(lat_n) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # Mean radius of earth in km: 6371
    great_circle_distance = c * 6371
    # Minimum map tile size 256 x 256 pixels
    zoom = math.floor(8 - math.log(bound_factor * great_circle_distance / math.sqrt(2 * 256 * 256))) / math.log(2)
    # Convert zoom level from float to integer
    zoom = int(zoom)
    print(great_circle_distance, clat, clng, zoom)
    return clat, clng, zoom


def main():
    filename = file_selector()
    file_version(filename)
    # list_all_signals(filename)
    cfg = read_config('config_signals.json')
    signals, dtype, norm, raw, t = data_prep(filename, cfg, freq=0.01)
    return


if __name__ == "__main__":
    main()
