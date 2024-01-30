import pandas as pd

class DataTransformer:
    """
    Transforms data into a unnested dataframe.
    """

    def __init__(self):
        pass

    def transform_data(self, data):
        """
        Transforms input data (json or dataframe) into an unnested dataframe.

        Args:
        - data: Input json data or dataframe.

        Returns:
        - data: transformed data in a table format (dataframe)
        """
        if not isinstance(data, pd.DataFrame):
            df = pd.json_normalize(data)
        else:
            df = data

        self._explode_lists(df)
        self._expand_dicts(df)
        self._drop_empty_columns(df)

        return df

    def _explode_lists(self, df):
        """
        Explodes columns containing lists in the dataframe.

        Args:
        - df: Input dataframe.
        """
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df = df.explode(col)

    def _expand_dicts(self, df):
        """
        Expands columns containing dictionaries in the dataframe.

        Args:
        - df: Input dataframe.
        """
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df = pd.concat([
                    df.drop([col], axis=1),
                    df[col].apply(pd.Series)
                    .rename(columns={
                        "delay": f"{col}_delay",
                        "time": f"{col}_time",
                        "uncertainty": f"{col}_uncertainty"
                    })], axis=1)

    def _drop_empty_columns(self, df):
        """
        Drops columns that contain only NaN values.

        Args:
        - df: Input dataframe.
        """
        cols_to_drop = df.columns[df.isnull().all()]
        df = df.drop(cols_to_drop, axis=1)