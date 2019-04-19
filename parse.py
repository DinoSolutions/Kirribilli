import pandas
from asammdf import MDF, Signal
from prototype import (
    TEST_FILES,
    SIGNALS,
)


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


def signal_index(signalname, filename):
    with MDF(filename) as mdf_file:
        counter = 0
        for channel in mdf_file.iter_channels():
            if channel.name == signalname:
                return counter
            else:
                counter += 1
        return -1  # Error: signalname not found in MDF file


def signal_values(signalnames, filename):
    with MDF(filename) as mdf_file:
        channels = mdf_file.select(signalnames, dataframe=False)
        return channels


def main():
    # f = file_selector()
    f = TEST_FILES[0]
    # file_version(f)
    # list_all_signals(f)
    # lat_name = 'GPS_Lat'
    # lng_name = 'GPS_Lon'
    # lat_index = signal_index(lat_name, f)
    # lng_index = signal_index(lng_name, f)
    # lat_timestamps, lat_values = signal_values([lat_name], f)
    # lng_timestamps, lng_values = signal_values([lng_name], f)
    channels = signal_values(SIGNALS, f)
    print(channels)


if __name__ == "__main__":
    main()
