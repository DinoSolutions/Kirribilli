# import pandas
import numpy
from asammdf import MDF, Signal
from prototype import (
    TEST_FILES,
    SLOW_SIGNALS,
    SIGNALS,
)
from collections import namedtuple


def file_selector():
    input_file = ''  # dummy function for now
    return input_file


def file_version(filename):
    with MDF(filename) as mdf_file:
        print(mdf_file.version)  # pylint: disable=no-member
    return mdf_file.version  # pylint: disable=no-member


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
        # TODO: Get signal timestamps and samples as numpy arrays
        for sig in mdf_fast:
            print(sig.name, sig.timestamps[0:3], sig.samples[0:3])
        print('\n---SPLIT---\n')
        for sig in mdf_slow:
            print(sig.name, sig.timestamps[0:3], sig.samples[0:3])
        # TODO: Construct GPS coordinates
        lat_index = slow_signals.index('GPS_Lat')
        lng_index = slow_signals.index('GPS_Lon')
        speed_index = slow_signals.index('GPS_Velocity')
        lat_value = mdf_slow[lat_index].samples
        lng_value = mdf_slow[lng_index].samples
        gps_points = numpy.array([lng_value, lat_value]).transpose()  # for GeoJSON: [longitude, latitude]
        gps_time = numpy.array(mdf_slow[speed_index].timestamps)
        lat1 = numpy.min(mdf_slow[lat_index].samples)
        lat2 = numpy.max(mdf_slow[lat_index].samples)
        lng1 = numpy.min(mdf_slow[lng_index].samples)
        lng2 = numpy.max(mdf_slow[lng_index].samples)
        bound_s = lat1 - (lat2 - lat1) * 0.05
        bound_n = lat2 + (lat2 - lat1) * 0.05
        bound_w = lng1 - (lng2 - lng1) * 0.05
        bound_e = lng2 + (lng2 - lng1) * 0.05
        lat_c = (lat2 - lat1) / 2
        lng_c = (lng2 - lng1) / 2
        # gps_boundaries = numpy.array([bound_s, bound_n, bound_w, bound_e, lat_c, lng_c])
        Boundaries = namedtuple('Boundaries', ['s', 'n', 'w', 'e', 'lat_c', 'lng_c'])
        gps_boundaries = Boundaries(s=bound_s, n=bound_n, w=bound_w, e=bound_e, lat_c=lat_c, lng_c=lng_c)
        return gps_time, gps_points, gps_boundaries


def main():
    # f = file_selector()
    f = TEST_FILES[0]
    # file_version(f)
    # list_all_signals(f)

    # -- TO DELETE:
    # lat_name = 'GPS_Lat'
    # lng_name = 'GPS_Lon'
    # lat_index = get_signal_index(lat_name, f)
    # lng_index = get_signal_index(lng_name, f)
    # --

    # lat_timestamps, lat_values = signal_values([lat_name], f)
    # lng_timestamps, lng_values = signal_values([lng_name], f)
    # channels = signal_values(SIGNALS, f)
    # print(channels)
    gps_data = data_scrub(f, SLOW_SIGNALS, SIGNALS, slow_freq=1, fast_freq=0.01)
    return


if __name__ == "__main__":
    main()
