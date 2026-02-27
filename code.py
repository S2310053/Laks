## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader
import matplotlib.pyplot as plt


##
# Loads the data
#
loadData              = DataLoader()
dataSalmonPrice       = loadData.FishPoolData()
dataCpi               = loadData.CPIData()
dataEurnok            = loadData.EURNOKData()
dataSalmonExportPrice = loadData.SSBPriceData()
dataEscapes           = loadData.EscapesData()
dataBiomass           = loadData.BiomassData()
dataPigPrice          = loadData.PigPriceData()

print(dataPigPrice.info())
print(dataPigPrice.columns)

#print(dataEscapes.columns)