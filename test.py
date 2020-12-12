import json
import pandas as pd

with open('data.json') as f:
 	data = json.load(f)

print(data['regions']['Finland']['models']['CanESM5']['level']['three'])

df = pd.DataFrame()
for model in ['CanESM5','MIROC6']:
#for model in ['MIROC6']:
	for lev in ['1','1.5','2','3','4']:
		print('Load data: ','data_'+model+"_"+lev+".csv")
		df_ = pd.read_csv('data_'+model+"_"+lev+".csv")
		if len(df) ==0:
			df=df_
		else:
			df=df.append(df_)

print(df[(df.region=='Finland') & (df.model=='CanESM5') & (df.lev == 3)].tasmax.tolist())
