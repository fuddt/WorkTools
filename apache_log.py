


from typing import Any
import pandas as pd
import polars as pl


class ApacheLog(object):
    def __init__(self, logFilePath: str):
        logFile = pl.read_csv(logFilePath, has_header=False, separator=" ",infer_schema_length=10000, truncate_ragged_lines=True)
        logFile = self._process_column(logFile)
        logFile = self._process_url(logFile)
        self.logFile = pl.from_pandas(logFile)

    def __str__(self) -> str:
        return str(self.logFile)

    @staticmethod
    def _process_column(df: pl.DataFrame) -> pl.DataFrame:
        #日付から日付のみを取得する
        def get_day( value):
            return value[1:3]
        # 日付から無駄を除外する
        def remove_date(value):
            return value[13:]

        df = df.select(pl.col(['column_1','column_4', 'column_6', 'column_7', 'column_8',
                            'column_9', 'column_10', 'column_11', 'column_12', 'column_13']))
        # 日付の処理をする
        df =  df.with_columns(pl.col("column_4").map_elements(get_day).alias("Day"),
                            pl.col("column_4").map_elements(remove_date).alias("Time"))

        # データ成型第1段階
        df = df.select(pl.col([ 'column_1', 'Day','Time','column_6', 'column_7', 'column_8', 
                                'column_9', 'column_10','column_11', 'column_12', 'column_13']))
        # カラム名をつけてあげる
        df.columns = ['IPaddress/LB', 'Day', 'Time','URL','RequestSTATE', 'ResponseSize', 
                        'RequestProcessingTime', 'Referer', 'UserAgent', 'transmissionSize','Cookie']

        return df

    @staticmethod
    def _process_url(df: pl.DataFrame) -> pl.DataFrame:        
        data_pd = df.to_pandas()
        data_pd = data_pd["URL"].str.split(" " ,expand=True).drop(2, axis=1)
        data_pd.columns = ["GET/POST", "URL"]
        
        # 結合する
        data = df.to_pandas().drop("URL", axis=1)
        data = pd.concat([data, data_pd], axis=1)
        return data.loc[:,['IPaddress/LB', 'Day', 'Time', 'GET/POST', 'URL','RequestSTATE', 'ResponseSize', 'RequestProcessingTime',
                        'Referer', 'UserAgent', 'transmissionSize','Cookie']]
        
    #日付でフィルター
    def filter_by_day(self, day: str) -> pl.DataFrame:
        day = int(day)
        return (self.logFile.with_columns(pl.col("Day").cast(pl.Int32))
                                        .filter(pl.col("Day") == day))
    # 時間でフィルター
    def filter_by_time(self, from_time: str, end_time: str) -> pl.DataFrame:
        return self.logFile.filter((pl.col("Time") >= from_time) & (pl.col("Time") <= end_time))

    def to_csv(self, filePath: str):
        self.logFile.to_pandas().to_csv(filePath, index=False)