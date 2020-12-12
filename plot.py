import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import json

df = pd.DataFrame()

for model in ['CanESM5','MIROC6']:
#for model in ['MIROC6']:
	for lev in ['1','1.5','2','3','4']:
		#print('Load data: ','data_'+model+"_"+lev+".csv")
		df_ = pd.read_csv('data_'+model+"_"+lev+".csv")
		#print(df_)
		if len(df) ==0:
			df=df_
		else:
			df=df.append(df_)

json_out={}
json_out['regions'] = {}
for region in df.region.unique():
	json_out['regions'][region] = {}
	json_out['regions'][region]['models'] = {}
	for model in ['CanESM5','MIROC6']:
		json_out['regions'][region]['models'][model] = {}
		json_out['regions'][region]['models'][model]['level'] = {}
		for lev in ['one','half','two','three','four']:
			lev_tmp=0
			if lev == 'one':
				lev_tmp=1
			if lev == 'half':
				lev_tmp=1.5
			if lev == 'two':
				lev_tmp=2
			if lev == 'three':
				lev_tmp=3
			if lev == 'four':
				lev_tmp=4
			df_tmp = df[(df.region==region) & (df.model==model) & (df.lev == lev_tmp)]
			json_out['regions'][region]['models'][model]['level'][lev] = {}
			json_out['regions'][region]['models'][model]['level'][lev]['tmax'] = (df_tmp['tasmax']-273.15).tolist()
			json_out['regions'][region]['models'][model]['level'][lev]['year'] = df_tmp['year'].tolist()
			json_out['regions'][region]['models'][model]['level'][lev]['global_mean'] = df_tmp['global_mean'].tolist()

print(df[(df.region=='Finland') & (df.model=='CanESM5') & (df.lev ==1)])
print(json_out['regions']['Finland']['models']['CanESM5']['level']['one'])

with open('data.json', 'w') as outfile:
    json.dump(json_out, outfile)

with open('data.json') as f:
 	data = json.load(f)

print(data['regions']['Finland']['models']['CanESM5']['level']['one'])

