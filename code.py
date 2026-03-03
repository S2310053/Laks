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
#dataSalmonPrice       = loadData.SalmonPriceFP()
#dataInflation          = loadData.Inflation()
#dataEurnok            = loadData.Eurnok()
#dataSalmonExportPrice = loadData.SalmonPriceSSB()
#dataEscapes            = loadData.Escapes()
#dataBiomass           = loadData.Biomass()
#dataPigPrice          = loadData.PigPriceData()
DATA = loadData.Data()

#print(dataEurnok.info())
#print(dataEurnok.columns)

print(DATA)
print(DATA.info())

DATA.to_excel(r"C:\Users\arzol\OneDrive\Escritorio\my_dataset.xlsx", index=False)
