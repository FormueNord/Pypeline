import datetime


def function_constructor(func):
    """
    Decorator used to construct a function
    """
    def wrapper(*args, **kwargs):
        return lambda file_paths: func(file_paths, **kwargs)
    return wrapper

def strings_to_dates(df,datetime_transformer = lambda x: datetime.datetime.strptime(x,"%d/%m/%Y").date()):
    for column in df.columns:
        if "date" in column.lower():
            df[column] = df[column].apply(datetime_transformer)
    return df
            