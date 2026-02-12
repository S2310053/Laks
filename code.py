## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader


##
# Loads the data
#
loadData              = DataLoader()
dataSalmonPrice       = loadData.loadFishPoolData()
dataCpi               = loadData.loadCPIData()
dataEurnok            = loadData.loadEURNOKData()
dataSalmonExportPrice = loadData.loadSSBPriceData()
dataEscapes           = loadData.loadEscapesData()
dataBiomass           = loadData.loadBiomassData()
dataPigPrice          = loadData.loadPigPriceData()

print(dataSalmonExportPrice) 