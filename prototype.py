TEST_FILES = (
    'data/Acura_M52_NB_Comfort.MF4',
    'data/Jag_Huron_EB.MF4',
    'data/Jag_Huron_WB.MF4',
    'data/Niro_20kph.dat',
)

GPS_SIGNALS = (
    'GPS_Lat',
    'GPS_Lon',
    'GPS_Alt',
    'GPS_Time',
    'GPS_SatCount',
    'GPS_Velocity',
)

VEHICLE_SIGNALS = (
    'Speed_KPH',
    'EBB_BRAKE_PEDAL_ON',
    'ENG_ACCELE_PEDAL_POSITION',
    'TM_GEAR_POSITION_TARGET',
    'TM_GEAR_POSITION_ACTUAL',
    'SRS_LAT_G',
    'SRS_LON_G',
)

ADAS_SIGNALS = (
    'ACC_DISPLAY_SPEED_30C',
    'ACC_REQ_ACC_IND',
    'ACC_REQ_DISTANCE_IND',
    'LKAS_STATUS_ACTIVE_33D',
)

DYN_SIGNALS = (
    'AccelerationChassis',
    'AccelerationLateral',
    'AccelerationVertical',
    'ACC_F_X',
    'ACC_F_Y',
    'ACC_F_Z',
    'OMEGA_F_Z',
    'Yaw_Rate_AVL',
)

LANE_SIGNALS = (
    'LaneCurvature_L_DMU',
    'LaneCurvature_R_DMU',
    'LaneDetectionQuality_L_DMU',
    'LaneDetectionQuality_R_DMU',
    'LaneDistance_L_DMU',
    'LaneDistance_R_DMU',
    'LaneYawAngle_L_DMU',
    'LaneYawAngle_R_DMU',
)

ALL_SIGNALS = GPS_SIGNALS \
              + VEHICLE_SIGNALS \
              + ADAS_SIGNALS \
              + DYN_SIGNALS \
              + LANE_SIGNALS

SLOW_SIGNALS = [signal for signal in GPS_SIGNALS]

SIGNALS = [signal for signal in ALL_SIGNALS]

ALL = ['time'] + SIGNALS
