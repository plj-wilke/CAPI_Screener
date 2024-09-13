import json, requests
import pandas as pd
import re
import json
grant_scope = {"grant_type": "api-user", "scope": "pub.surveys pub.hubs"}
authEndpoint = "https://idp.euro.confirmit.com/identity/connect/token"
CLIENT_ID = "dcc51cc9-f2be-492a-a475-c13947773b6c"
CLIENT_SECRET = "fd2c6a40-45ee-4978-8683-e5a46d8582a6"
# P = "p708273210641"
page_size = 500

# P = "p1878268087"
# P = "p828694300218"
P = "p246067733184"
req = requests.post(authEndpoint, data=grant_scope, auth=(CLIENT_ID, CLIENT_SECRET))
if req.status_code == 200:
    respText = json.loads(req.text)
    access_token = respText["token_type"] + " " + respText["access_token"]

def GetLanguages(url, token):
    # params = {"pageSize": page_size}
    req = requests.get(url, headers = {"accept": "application/json", "authorization": token})#, params=params)
    if req.status_code == 200:
        respText = json.loads(req.text)
        next_page = respText["links"].get("nextpage")
    return respText, next_page

def GetData(url, token):
    params = {"pageSize": page_size}
    req = requests.get(url, headers = {"Content-Type": "application/json", "authorization": token}, params=params)
    if req.status_code == 200:
        respText = json.loads(req.text)
        next_page = respText["links"].get("nextpage")
    return respText, next_page

def confirmit_to_df(P):
    # languages, _ = GetLanguages(f'https://horizons.confirmit.eu/v1/surveys/languages',access_token)
    schema, next_page = GetData(f'http://ws.euro.confirmit.com/v1/surveys/{P}/responses/schema',access_token)
    schema_df = pd.DataFrame(schema["root"]["variables"])
    languages = [x.get("confirmitLanguageId") for x  in schema.get("languages")]
    data, next_page = GetData(f'http://ws.euro.confirmit.com/v1/surveys/{P}/responses/data',access_token)
    total = (data["totalCount"] // page_size)+1
    test, test2 = GetData(f'http://ws.euro.confirmit.com/v1/surveys/{P}/quotas',access_token)
    counter = 1
    df = pd.DataFrame(data["items"])

    print(f"Total rows of data : {data["totalCount"]}")
    print(f"Loaded {counter} out of {total} pages of data")

    try:
        while next_page is not None:
            data, next_page = GetRespondentData(next_page, access_token)
            df = pd.concat([df, pd.DataFrame(data["items"])])
            counter += 1
            print(f"Loaded {counter} out of {total}  pages of data")

    except Exception as e:
        print(f"Hit an exception {e}")

    # Check if responseid exists in df
    schema_df = schema_df[[x in list(df.columns) for x in list(schema_df["name"])]]

    # Schema cleaned
    fields_to_keep = ["name", "variableType", "texts", "fields", "options"]
    schema_df = schema_df[fields_to_keep]
    for field in fields_to_keep:
        for language in languages:
            schema_df[field] = schema_df[field].astype("string").replace(f"'languageId': {language}, ","", regex=True)        
        schema_df[field] = schema_df[field].replace(to_replace='\\', value='')
        schema_df[field] = schema_df[field].replace(to_replace=';', value=',')
        schema_df[field] = schema_df[field].replace(to_replace="'", value='"', regex=True)
        schema_df[field] = schema_df[field].replace(to_replace='\\\\n', value='', regex=True) # removes \n
        schema_df[field] = schema_df[field].replace(to_replace='False', value='"False"', regex=True)
        schema_df[field] = schema_df[field].replace(to_replace='True', value='"True"', regex=True)
        schema_df[field] = schema_df[field].replace(to_replace='&amp;', value='&', regex=True)
        schema_df[field] = schema_df[field].replace(to_replace='</?[^>]*>', value='', regex=True) # Removes e.g <\u> or <p> html brackets
        schema_df[field] = schema_df[field].replace(to_replace='  ', value=' ', regex=True)
        schema_df[field] = schema_df[field].replace(to_replace='\\b["]+(?!\S)*\\b', value='', regex=True) # Removes " between two chracters, e.g Field's -> Fields
        schema_df[field] = schema_df[field].replace(to_replace='\^.*\^', value='SELECTION', regex=True) # Removes everything included in ^text^ e.g ^f("SCR1")^
        # schema_df[field] = schema_df[field].fillna("[]") # Fills with empty list, makes it easier in Power BI

    if "responseid" in df:
        responseid_df = pd.DataFrame([["responseid", "numeric", None, None, None]], columns=['name','variableType', "texts", "fields", "options"])
        schema_df = pd.concat([responseid_df, schema_df], ignore_index=True)
    else:
        pass

    # Removes all columns not in schema from the data frame
    # This could e.g be old data not recorded anymore
    df = df[df.columns.intersection(list(schema_df["name"]))]

    date_fields = schema_df.loc[schema_df["variableType"].isin(["dateTime"]), "name"] # Convert to datetime
    for date_field in date_fields:
        df[date_field] = pd.to_datetime(df[date_field], format='ISO8601', utc=True)

    # "numeric", "singleChoice" can be int, float, string or unknown. Leave for now
    numeric_fields = schema_df.loc[schema_df["variableType"].isin(["numeric"]), "name"] # Convert to numeric
    for numeric_field in numeric_fields:
        try:
            df[numeric_field] = pd.to_numeric(df[numeric_field])
        except: # json format
            pass
    #         df[numeric_field] = df[numeric_field].astype(pd.StringDtype())

    # # singleChoice might be numeric, if that is the case, convert it is powerbi
    # string_fields = schema_df.loc[~schema_df["variableType"].isin(["numeric", "dateTime"]), "name"] # Convert to string
    # for string_field in string_fields:
    #     df[string_field] = df[string_field].astype(pd.StringDtype())



    # "numeric", "singleChoice" can be int, float, string or unknown. Leave for now

    # Schema cleaned
    for field in list(df.columns):
        df[field] = df[field].replace(to_replace='\\', value='')
        df[field] = df[field].replace(to_replace=';', value=',', regex=True)
        df[field] = df[field].replace(to_replace=',', value='', regex=True)
        df[field] = df[field].replace(to_replace="'", value='"', regex=True)
        df[field] = df[field].replace(to_replace="\r\n", value='', regex=True)
        df[field] = df[field].replace(to_replace='\\\\n', value='', regex=True) # removes \n
        df[field] = df[field].replace(to_replace='False', value='"False"', regex=True)
        df[field] = df[field].replace(to_replace='True', value='"True"', regex=True)
        df[field] = df[field].replace(to_replace='&amp;', value='&', regex=True)
        df[field] = df[field].replace(to_replace='</?[^>]*>', value='', regex=True) # Removes e.g <\u> or <p> html brackets
        df[field] = df[field].replace(to_replace='\\b["]+(?!\S)*\\b', value='', regex=True) # Removes " between two chracters, e.g Field's -> Fields
        df[field] = df[field].replace(to_replace='\^.*\^', value='SELECTION', regex=True) # Removes everything included in ^text^ e.g ^f("SCR1")^
        df[field] = df[field].replace(to_replace='  ', value=' ', regex=True)

    # Data cleaning
    df = df.loc[df["status"]=="complete"] # Removes incomplete and screended.
    df["interview_length"] = df["interview_end"]-df["interview_start"]

    return df
# schema_df.to_csv("schema_df.csv", index=False)
# df.to_csv("df.csv", index=False)

###