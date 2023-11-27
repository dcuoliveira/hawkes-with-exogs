##Extracting data:
# import py7zr
#
# with py7zr.SevenZipFile('/cs/academic/phd3/konajain/data/AAPL_2019-01-01_2020-09-27_10.7z', mode='r') as z:
#     z.extractall()
import datetime as dt
import pickle
from hawkes import dataLoader, fit
import pandas as pd
import numpy as np

def main():
    ric = "AAPL.OQ"
    sDate = dt.date(2019,1,2)
    eDate = dt.date(2019,1,2)
    for d in pd.date_range(sDate, eDate):
        l = dataLoader.Loader(ric, d, d, nlevels = 2) #, dataPath = "/home/konajain/data/")
        data = l.load12DTimestamps()
        #df = pd.read_csv(l.dataPath+"AAPL.OQ_2020-09-14_12D.csv")
        #df = df.loc[df.Time < 100]
        #eventOrder = np.append(df.event.unique()[6:], df.event.unique()[-7:-13:-1])
        #data = {'2020-09-14' : list(df.groupby('event')['Time'].apply(np.array)[eventOrder].values)}
        cls = fit.ConditionalLeastSquaresLogLin(data, loader = l)
        cls.runTransformDate()
        # with open(l.dataPath + ric + "_" + str(sDate) + "_" + str(eDate) + "_CLSLogLin" , "wb") as f: #"/home/konajain/params/"
        #     pickle.dump(thetas, f)
    return 0
    # ric = "AAPL.OQ"
    # d = dt.date(2020,9,14)
    # l = dataLoader.Loader(ric, d, d, nlevels = 2, dataPath = "/home/konajain/data/")
    # #a = l.load12DTimestamps()
    # df = pd.read_csv("/home/konajain/data/AAPL.OQ_2020-09-14_12D.csv")
    # eventOrder = np.append(df.event.unique()[6:], df.event.unique()[-7:-13:-1])
    # timestamps = [list(df.groupby('event')['Time'].apply(np.array)[eventOrder].values)]
    # cls = fit.ConditionalLaw(timestamps)
    # params = cls.fit()
    # with open("/home/konajain/params/" + ric + "_" + str(d) + "_" + str(d) + "_condLaw" , "wb") as f: #"/home/konajain/params/"
    #     pickle.dump(params, f)
    # return params

main()
