import pandas as pd

# df = pd.DataFrame(columns = ["msr_id","msr_no","muster_roll_period_from","muster_roll_period_to","panchayat_code","payment_date","tot_persondays","total_dues","done"])

# chunksize = 10 ** 6
# for chunk in pd.read_csv("/Users/parthchawla/musters.csv", chunksize=chunksize, names=["msr_id","msr_no","muster_roll_period_from","muster_roll_period_to","panchayat_code","payment_date","tot_persondays","total_dues","done"]):
#     chunk["muster_roll_period_to"] = pd.to_datetime(chunk["muster_roll_period_to"])
#     chunk["muster_roll_period_from"] = pd.to_datetime(chunk["muster_roll_period_from"])
#     chunk = chunk[(chunk.muster_roll_period_to >= "2016-04-01") & (chunk.muster_roll_period_to <= "2016-04-30")]
#     df = df.append(chunk)

# df.to_csv("/Users/parthchawla/musters_new.csv")

df = pd.read_csv("/Users/parthchawla/musters_new.csv")
df["payment_date"] = pd.to_datetime(df["payment_date"])
df.to_csv("/Users/parthchawla/musters_new1.csv")