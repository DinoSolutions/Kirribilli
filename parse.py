from asammdf import MDF

fileAcura = 'data/Acura_M52_NB_Comfort.MF4'
fileJagEast = 'data/Jag_Huron_EB.MF4'
fileJagWest = 'data/Jag_Huron_WB.MF4'

mdf = MDF(fileAcura)
print(mdf.version) # pylint: disable=no-member
