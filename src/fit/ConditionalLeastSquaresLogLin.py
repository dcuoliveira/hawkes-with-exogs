import numpy as np
import pandas as pd
import pickle
import gc

class ConditionalLeastSquaresLogLin():

    def __init__(self, dictBinnedData, **kwargs):
        self.dictBinnedData = dictBinnedData
        self.dates = list(self.dictBinnedData.keys())
        self.cfg = kwargs
        self.cols = ["lo_deep_Ask", "co_deep_Ask", "lo_top_Ask","co_top_Ask", "mo_Ask", "lo_inspread_Ask" ,
                     "lo_inspread_Bid" , "mo_Bid", "co_top_Bid", "lo_top_Bid", "co_deep_Bid","lo_deep_Bid" ]
        # df = pd.read_csv("/home/konajain/data/AAPL.OQ_2020-09-14_12D.csv")
        # eventOrder = np.append(df.event.unique()[6:], df.event.unique()[-7:-13:-1])
        # timestamps = [list(df.groupby('event')['Time'].apply(np.array)[eventOrder].values)]
        # list of list of 12 np arrays

    def transformData(self, timegrid, date, arrs):
        print(date)
        timegrid_new = np.floor(timegrid/(timegrid[1] - timegrid[0])).astype(np.longlong)
        ser = []
        # bins = np.arange(0, np.max([np.max(arr) for arr in arrs]) + 1e-9, (timegrid[1] - timegrid[0]))
        for arr, col in zip(arrs, self.cols):
            print(col)
            arr = arr[::-1]
            assignedBins = np.ceil(arr/(timegrid[1] - timegrid[0])).astype(np.longlong)
            binDf = np.unique(assignedBins, return_counts = True)
            binDf = pd.DataFrame({"bin" : binDf[0], col : binDf[1]})
            binDf = binDf.set_index("bin")
            #binDf = binDf.reset_index()
            ser += [binDf]
        print("done with binning")
        df = pd.concat(ser, axis = 1)
        df = df.fillna(0)
        df = df.sort_index(ascending=False)
        del arrs
        gc.collect()
        res = []
        try:
            with open(self.cfg.get("loader").dataPath + self.cfg.get("loader").ric + "_" + str(date) + "_" + str(date) + "_" + str(len(timegrid)) +  "_inputRes" , "rb") as f: #"/home/konajain/params/"
                while True:
                    try:
                        res.append(len(pickle.load(f)))
                    except EOFError:
                        break
        except:
            print("no previous data cache found")
        restartIdx = int(np.sum(res))
        res = []

        for i in range(restartIdx + 1, len(df) - 1):

            idx = df.index[i]
            bin_index_new = np.searchsorted(timegrid_new, idx - df.index, side="right")
            last_idx = len(timegrid_new)

            df['binIndexNew'] = bin_index_new
            df_filtered = df[df['binIndexNew'] != last_idx] # remove past > last elt in timegrid

            # unique_bins, bin_counts = np.unique(df_filtered['binIndexNew'], return_counts=True)

            bin_df = np.zeros((len(timegrid_new) - 1, len(self.cols)))
            df_filtered = df_filtered.loc[df_filtered.index[i+1]:]
            # rCurr = df.iloc[i].values[:-1]
            for j, col in enumerate(self.cols):

                bin_df[:, j] = np.bincount(df_filtered['binIndexNew'], weights=df_filtered[col], minlength=len(timegrid_new))[1:]
                # tmp = rCurr.copy()

            lags = bin_df
            res.append([df.loc[idx, self.cols].values, lags])

            if i%5000 == 0 :
                print(i)
                with open(self.cfg.get("loader").dataPath + self.cfg.get("loader").ric + "_" + str(date) + "_" + str(date) + "_" + str(len(timegrid)) + "_inputRes" , "ab") as f: #"/home/konajain/params/"
                    pickle.dump(res, f)
                res =[]
                gc.collect()
            elif i==len(df)-2:
                with open(self.cfg.get("loader").dataPath + self.cfg.get("loader").ric + "_" + str(date) + "_" + str(date) + "_" + str(len(timegrid)) + "_inputRes" , "ab") as f: #"/home/konajain/params/"
                    pickle.dump(res, f)
                res =[]
                gc.collect()

        return res