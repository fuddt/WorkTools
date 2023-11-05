import pandas as pd
import polars as pl

class ApacheLogAnalys(object):
    def __init__(self, csvFilePath: str):
        df = pd.read_csv(csvFilePath)
        self.log = self._preprocess(df)
        self.calc_result = None

    def separate_by_time(self, time, col):
        #時間列の処理
        data = self.time_preprocess(self.log)
        data = self.split_time(data ,time, col)
        self.log = self.refact_time(data)


    def calculate_data(self, 
                       groupby_columns: str, 
                       target_column: str,
                       calc_count=0,
                       calc_sum=0,
                       calc_mean=0,
                       calc_max=0,
                       calc_min=0,
                       calc_std=0
                       ) -> pl.DataFrame:
        groupby_columns = groupby_columns.split(",")
        groupby_columns = [s.strip() for s in groupby_columns]

        calculation_select = [calc_count, calc_sum, calc_mean, calc_max, calc_min, calc_std]
        calculation_patern = [
            pl.col(target_column).count().alias("count"),
            pl.col(target_column).sum().alias("sum"),
            pl.col(target_column).mean().alias("mean"),
            pl.col(target_column).max().alias("max"),
            pl.col(target_column).min().alias("min"),
            pl.col(target_column).std().alias("std"),
        ]
        
        calculation = [calculation_patern[i] for i, select in enumerate(calculation_select) if select == 1] 
        self.calc_result = self.log.group_by(groupby_columns).agg(*calculation)
    
    def to_csv(self, filePath: str):
        self.calc_result.to_pandas().to_csv(filePath, index=False)


    @staticmethod
    def _preprocess(df):
        df['ResponseSize'] = df['ResponseSize'].replace("-", 0).astype(int)
        return pl.from_pandas(df)
    
    @staticmethod
    def split_time(data, time, col):
        time = int(time)
        data = data.with_columns(pl.col("hour").cast(pl.Int32),
                                 pl.col("minutes").cast(pl.Int32),
                                 pl.col("seconds").cast(pl.Int32))

        if col == "minutes":
            data = data.with_columns(((pl.col(col)//time) * time).alias(col),
                                     pl.lit(0).cast(pl.Int32).alias("seconds"))
        elif col == "hour":
            data = data.with_columns(((pl.col(col)//time) * time).alias(col),
                                     pl.lit(0).cast(pl.Int32).alias("minutes"),
                                     pl.lit(0).cast(pl.Int32).alias("seconds"))
        else:
            data = data.with_columns(((pl.col(col)//time) * time).alias(col))
        return data
    
    @staticmethod
    def time_preprocess(df: pl.DataFrame) -> pl.DataFrame:
        df = df.to_pandas()
        seconds = df["Time"].str[-2:]
        minutes = df["Time"].str[-5:-3]
        hour = df["Time"].str[-8:-6]
        ori_col = df.columns.tolist()
        df = pd.concat([hour,minutes,seconds,df], axis=1)
        df.columns = ["hour", "minutes" , "seconds"] + ori_col
        df = pl.from_pandas(df)
        df = df.with_columns(pl.col("hour").cast(pl.Int32),
                                pl.col("minutes").cast(pl.Int32),
                                pl.col("seconds").cast(pl.Int32))
        return df
   
    @staticmethod
    def refact_time(df: pl.DataFrame) -> pl.DataFrame:
        df = df.to_pandas()
        col = ["hour","minutes","seconds"]
        for i in col:
            df[i] = df[i].astype(str).str.zfill(2)
        df["Time"] = df["hour"] + ":" + df["minutes"] + ":" + df["seconds"]
        df = pl.from_pandas(df)
        return df.select(pl.col('Time'),
                         pl.col('hour'), pl.col('minutes'), pl.col('seconds'), 
                         pl.col("IPaddress/LB"), pl.col('Day'),
                         pl.col('GET/POST'), pl.col('URL'), pl.col('RequestSTATE'), 
                         pl.col('ResponseSize'), pl.col('RequestProcessingTime'), pl.col('Referer'), 
                         pl.col('UserAgent'), pl.col('transmissionSize'), pl.col('Cookie'))
