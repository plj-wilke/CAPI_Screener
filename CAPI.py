from confirmit_to_dataframe import confirmit_to_df


import re



P = "p847479987057"

def gps_to_lat_long(gps_point):
    gps = re.search("\\((.*)\\)", gps_point).group(0)[1:-1]
    # gps = gps.replace(".",",")
    lat, long = gps.split(" ")
    return lat, long

df = confirmit_to_df(P)

df[["lat", "long"]] = df.apply(lambda x: gps_to_lat_long(x['GPS']), axis=1,result_type='expand')

df["Pid"]=P

df = df[['Pid', 'lat', 'long','INT_ID','status','interview_start','interview_length']]

df.to_csv("testformat.csv")
# GPS to Lat, long

