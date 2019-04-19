TEST_FILES = (
    'data/Acura_M52_NB_Comfort.MF4',
    'data/Jag_Huron_EB.MF4',
    'data/Jag_Huron_WB.MF4',
    'data/Niro_20kph.dat',
)

GPS_SIGNALS = ('GPS_Lat', 'GPS_Lon')
VEHICLE_SIGNALS = ()
ADAS_SIGNALS = ()
DYN_SIGNALS = ()
ACC_SIGNALS = ()
LKA_SIGNALS = ()

ALL_SIGNALS = GPS_SIGNALS \
              + VEHICLE_SIGNALS \
              + ADAS_SIGNALS \
              + DYN_SIGNALS \
              + ACC_SIGNALS \
              + LKA_SIGNALS

SIGNALS = [signal for signal in ALL_SIGNALS]

ALL = ['time'] + SIGNALS
