import pandas as pd

class Transform:

    def __init__(self):
        pass

    def json_to_dataframe(self, data):
        """
        json_to_dataframe method returns a dataframe unnested dataframe
        Args:
        - data: json data or dataframe
        """
        if not isinstance(data, pd.DataFrame):
            df = pd.json_normalize(data)
        else:
            df = data
    
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df = df.explode(col)
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df = pd.concat([
                    df.drop([col], axis=1), df[col].apply(pd.Series)
                    .rename(columns={
                        "delay" : f"{col}_delay", 
                        "time": f"{col}_time", 
                        "uncertainty": f"{col}_uncertainty"
                        })], axis=1)
        
        # drop the nan colu
        cols_to_drop = df.columns[df.isnull().all()]
        df = df.drop(cols_to_drop, axis=1)
        
        return df
