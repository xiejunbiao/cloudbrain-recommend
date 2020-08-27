aa = {'w': -2, 'q': -1, 'c':3}
aa = dict(tuple(sorted(aa.items(),key=lambda x: x[1], reverse=True)))
print(aa)
print(aa.items())
list1 = [0,2,0,4]
print(list1.index(2))
print(list(aa.keys()))

bb= ['a','b','w','e','c','q','d']
print([0 for _ in range(len(bb))])
list1 = list(aa.keys())+bb
list2 = list(set(list1))
list2.sort(key=list1.index)
print(list2)
import pandas as pd
df=pd.DataFrame([{'A':'a','B':'12','c':5,'d':1},{'A':'c','B':'12','c':4,'d':7},{'A':'13','B':'12','c':1,'d':1}])
temp_score = 0
for i in range(0, len(df)):
    if df.iloc[[i]].values[0][2] == 5:
        temp_score += df.iloc[[i]].values[0][3] * 0.01
    elif df.iloc[[i]].values[0][2] == 4:
        temp_score += df.iloc[[i]].values[0][3] * 0.01 * 0.1
    elif df.iloc[[i]].values[0][2] == 3:
        temp_score += df.iloc[[i]].values[0][3] * 0.01 * 0.01
    elif df.iloc[[i]].values[0][2] == 2:
        temp_score -= df.iloc[[i]].values[0][3] * 0.01 * 0.1
        # temp_score -= self._owner_eval_shop_data[
        #     self._owner_eval_shop_data.eval_level == 2]["count"].values[0] * 0.01
    else:
        temp_score -= df.iloc[[i]].values[0][3] * 0.01
cc = [0 for _ in range(len(bb))]
print(bb)
for i in range(len(bb)):
    try:
        cc[i] = df[df.A == bb[i]].d.values[0]
    #cc = [df[df.A == shop_code].d.values[0] for shop_code in bb]
    #print(cc)
    except IndexError:
        pass
    continue
print(cc)
a1 = [1.222,2.222,3.022]
a2 = [4.025,5.0001,6.3333]
a3 = [7.2222,8.2222,9.1111]
a22 = [6,6,6]
data = []
import numpy as np

a4 = np.add(np.array(a1),np.array(a22),np.array(a3))
a5 = np.subtract(a4,np.array(a2))
a5 =np.add(a5,np.array(a3)).tolist()
result = [x -y+t for x,y,t in np.array([a1,a2,a3]).T]

print(result)
import datetime
current_time = datetime.datetime.now()
print(current_time)
begin_time = current_time + datetime.timedelta(days=-2)
begin_time = begin_time.strftime("%Y-%m-%d")
end_time = current_time.strftime("%Y-%m-%d")
mysql = "select shop_code from cb_shop_rank_info where created_time between '%s' and '%s'" % (begin_time, end_time)
print(mysql)
print(datetime.time())
print("start full_init at {}------------------------".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
