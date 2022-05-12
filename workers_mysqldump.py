import subprocess
import pandas as pd
import time

def chunker(lst, n):
    return (lst[pos:pos + n] for pos in xrange(0, len(lst), n))

df = pd.read_csv("/Users/parthchawla/musters_new.csv")
df = df.loc[df['done'] == 1]
ids = df['msr_id'].tolist()
print 'No. of musters with workers pulled:',len(ids)

chunked = []
for chunk in chunker(ids, 20):
    chunked.append(chunk)
print 'No. of chunks:',len(chunked)

with open('workers.sql','w') as output:
    for i,chunk in enumerate(chunked):
        print 'Chunk',i+1
        tup = tuple(chunk)
        subprocess.Popen('mysqldump --where="id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s or id=%s" vv_pull_workers workers' % (tup), stdout=output, shell=True)
        time.sleep(1)

# Filter INSERT lines:

# f = open("/Users/parthchawla/Desktop/workers.sql", "r")
# copy = open("/Users/parthchawla/Desktop/insertlines.sql", "w")

# for line in f:
#     if line.startswith("INSERT INTO") is True:
#         copy.write(line)

# f.close()
# copy.close()
