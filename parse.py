from os import listdir
from os.path import isfile, join
import re
import numpy
import json
from collections import namedtuple
from timeit import timeit

from asammdf import MDF, Signal

from prototype import TEST_FILES


def read_config(config):
    try:
        with open(config) as config_file:
            cfg = json.load(config_file)
            print('Config file [%s] loaded.' % config)
    except FileNotFoundError:
        print('Config file [%s] does not exist' % config)
        return
    return cfg


def file_selector():
    filename = TEST_FILES[0]
    print('Input Filename: %s' % filename)
    return [filename]


def file_version(filename):
    with MDF(filename) as mdf_file:
        # print('\nMDF Version: %s' % mdf_file.version)
        return mdf_file.version


def list_all_signals(filename):
    with MDF(filename) as mdf_file:
        counter = 0
        for channel in mdf_file.iter_channels():
            print(str(counter) + '\t' + channel.name)
            counter += 1
        print('\nTotal Number of Signals: ' + str(counter) + '\n')
    return counter


def get_signal_index(filename, signal_name):
    with MDF(filename) as mdf_file:
        counter = 0
        for channel in mdf_file.iter_channels():
            if channel.name == signal_name:
                return counter
            else:
                counter += 1
        return  # Error: signalname not found in MDF file


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


def read_mdf_data(filename, cfg=None, sample_rate=None):
    with MDF(filename) as mdf0_file:
        print('\nStarting to read MDF data...')
        # Number of all channels in MDF
        n_channels = sum(len(group.channels) for group in mdf0_file.groups)

        # If configuration is given, use only selected channels
        if cfg:
            sel_channel_std = list(cfg.keys())
            sel_channel_names = list(cfg.values())
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
    # TODO: the data type conversion table needs optimization for database efficiency
    conversion_table = {
        "<class 'numpy.uint8'>":    'int8',
        "<class 'numpy.uint16'>":   'int8',
        "<class 'numpy.uint32'>":   'int8',
        "<class 'numpy.int8'>":     'int8',
        "<class 'numpy.int16'>":    'int8',
        "<class 'numpy.float64'>":  'float8',
        "<class 'numpy.bytes_'>":   'text',
    }
    return conversion_table[data_type]


def map_init(lat_s, lat_n, lng_w, lng_e, bound_factor):
    import math
    clat = (lat_n + lat_s) / 2
    clng = (lng_e + lng_w) / 2
    dlat = math.radians(lat_n - lat_s)
    dlng = math.radians(lng_e - lng_w)

    # Haversine formula:
    # Calculate the great-circle distance between two points
    a = math.sin(dlat / 2) ** 2 + math.cos(lat_s) * math.cos(lat_n) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Mean radius of earth in km: 6371
    great_circle_distance = c * 6371

    # Minimum map tile size 256 x 256 pixels
    zoom = math.floor(8 - math.log(bound_factor * great_circle_distance / math.sqrt(2 * 256 * 256))) / math.log(2)

    # Convert zoom level from float to integer
    zoom = int(zoom)
    # print(great_circle_distance, clat, clng, zoom)
    return clat, clng, zoom


def main():
    filename = file_selector()
    table_name = re.search(r"\/*.*\/(.*)\.*\.", filename).group(1)
    config_name = 'config_' + table_name + '.json'
    file_version(filename)
    # list_all_signals(filename)
    cfg = read_config(config_name)
    # read_mdf_data(filename, cfg=cfg)
    read_mdf_data(filename)
    # print(timeit(lambda: data_prep_freq(filename), number=10))
    # print(timeit(lambda: data_prep_ch(filename), number=10))
    return


if __name__ == "__main__":
    main()
