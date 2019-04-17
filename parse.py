from asammdf import MDF, Signal
import pandas


def file_selector():
    file_acura = 'data/Acura_M52_NB_Comfort.MF4'
    # file_jag_east = 'data/Jag_Huron_EB.MF4'
    # file_jag_west = 'data/Jag_Huron_WB.MF4'
    # file_niro = 'data/Niro_20kph.dat'
    return file_acura


def file_version(filename):
    with MDF(filename) as mdf_file:
        print(mdf_file.version)  # pylint: disable=no-member
    return mdf_file.version


def list_signals(filename):
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


def signal_values(signalname, filename):
    with MDF(filename) as mdf_file:
        channel = mdf_file.select(signalname)
        return channel[0].timestamps, channel[0].samples


if __name__ == "__main__":
    f = file_selector()
    # file_version(f)
    list_signals(f)
    lat_name = 'GPS_Lat'
    lng_name = 'GPS_Lon'
    lat_index = signal_index(lat_name, f)
    lng_index = signal_index(lng_name, f)
    lat_timestamps, lat_values = signal_values([lat_name], f)
    lng_timestamps, lng_values = signal_values([lng_name], f)
    print(len(lat_values),len(lng_values))