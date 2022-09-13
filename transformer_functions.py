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
            if not all(("datetime" in str(type(value)) for value in df[column])):
                df[column] = df[column].apply(datetime_transformer)
    return df

def collateral_column_date_iterator(df, column_name, row_string):
    df.insert(0,column_name, None)
    date_value = None
    for i,string in enumerate(df.iloc[:,1]):
        df.iloc[i,0] = date_value
        if row_string in str(string):
            date_value = datetime.datetime.strptime(string[-15:][:8],"%Y%m%d").date()
        if df.iloc[i,1] == "State":
            df.iloc[i,0] = column_name
    return df

def collateral_transformer():
    def wrapper(df):
        df = collateral_column_date_iterator(df, "File_Date_ID", "Collateral Positions")
        df = df.iloc[2:]
        headers = df.iloc[0].str.translate(str.maketrans(" /","__"))
        headers = headers.str.replace("(","", regex = True)
        headers = headers.str.replace(")","", regex = True)
        headers = headers.str.replace("%","percent")
        headers = headers.str.replace("-","_")
        df.columns = headers
        df = df.iloc[1:,:21]
        df = df[~df["State"].isna()]
        df = df[["Collateral Positions" not in str(string) for string in df["State"]]]
        df = df[df["State"] != "State"]
        df = strings_to_dates(df)
        df.insert(1,"Upload_Date",datetime.datetime.now().date())

        #temporary until column names implemented in Azure Loader
        #df.insert(11, "Haircut_percent",0)
        #df.insert(12,"FX_haircut_percent",0)
        #df.drop("Valuation_percent", axis = 1, inplace = True)
        return df
    return wrapper

def exposure_column_date_iterator(df, column_name, row_string):
    df.insert(0,column_name, None)
    date_value = None
    for i,string in enumerate(df.iloc[:,1]):
        df.iloc[i,0] = date_value
        if row_string in str(string):
            date_value = datetime.datetime.strptime(df.iloc[i,2],"%Y-%m-%dT%H:%M:%S").date()
        if df.iloc[i,1] == "Trade Ref":
            df.iloc[i,0] = column_name
    return df

def exposure_transformer():
    def wrapper(df):
        #probably delete the function below when there's time for it. is redundant since expsure_date_ID is = File_Date_ID
        df = exposure_column_date_iterator(df, "File_Date_ID", "As at:")
        df = df[8:]
        headers = df.iloc[0].str.translate(str.maketrans(" /","__"))
        headers = headers.str.replace("(","", regex = True)
        headers = headers.str.replace(")","", regex = True)
        headers = headers.str.replace("%","percent")
        headers = headers.str.replace(" ","_")
        df.columns = headers
        df = df.iloc[1:]
        df.rename({"Exposure_Date":"Exposure_Date_ID"}, inplace = True, axis = 1)
        df = df[~df["Trade_Ref"].isna()]
        df = df[~df["Notional_1_Ccy"].isna()]
        df = df[df["File_Date_ID"] != "File_Date_ID"]
        df = strings_to_dates(df)
        df.insert(1,"Upload_Date",datetime.datetime.now().date())
        df["File_Date_ID"] = df["Exposure_Date_ID"]
        return df
    return wrapper

            