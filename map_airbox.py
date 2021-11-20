# -*- coding: utf-8 -*-
"""
Created on Thu May 30 00:18:19 2019

@author: 廷宇
"""

import json
import requests
import pandas as pd
import time
import datetime
import csv
import sys
import folium
import geopandas as gpd
import branca.colormap as cm
import numpy as np
from urllib.request import urlretrieve

#=================================================#
data = pd.DataFrame(columns=['ID','PM2.5','Latitude','Longitude'])

#中研院空氣盒子爬蟲
i=0
#下載檔案
data_url = "https://pm25.lass-net.org/data/last-all-airbox.json.gz"
urlretrieve(data_url,'/var/www/html/airbox_json/last-all-airbox.json.gz')
#解壓縮
import gzip
with gzip.open('/var/www/html/airbox_json/last-all-airbox.json.gz', 'rb') as f:
    last_data = f.read()
    row_data_2 = json.loads(last_data)
    for row_2 in range(len(row_data_2['feeds'])):
        data.loc[i,'ID'] = row_data_2['feeds'][row_2]['device_id']
        try:
            if round(row_data_2['feeds'][row_2]['s_d0'])<200:
                data.loc[i,'PM2.5'] = round(row_data_2['feeds'][row_2]['s_d0'])
            else:
                data.loc[i,'PM2.5'] = 0
        except:
            data.loc[i,'PM2.5'] = 0
        data.loc[i,'Latitude'] = row_data_2['feeds'][row_2]['gps_lat']
        data.loc[i,'Longitude'] = row_data_2['feeds'][row_2]['gps_lon']
        i=i+1 
num=len(data[data['PM2.5']!=0])
#============================================#
data.to_csv('/home/s05351035/Jupyter/data_airbox.csv',index=False)
#===================================================================================#
sys.setrecursionlimit(1000000)
taiwanmap_1x1 = gpd.read_file("/home/s05351035/Jupyter/Taiwan_1x1_map/Taiwan_1x1_map.shp",encoding='utf-8')
taiwanmap_1x1.crs = {'init' :'epsg:3826'}
taiwanmap_1x1=taiwanmap_1x1.to_crs(epsg=4326)
taiwanmap_1x1=taiwanmap_1x1.reset_index()

#===================================================================================#
#地圖打點
icon_color=['green','orange','red','purple']
name ='Sensor'+'('+str(num)+')'
station_1=folium.FeatureGroup(name=name,show = False)

with open('/home/s05351035/Jupyter/data_airbox.csv',newline='') as csvfile:
    rows = csv.reader(csvfile)
    for row in rows:
        try:
            row_Id = str(row[0])
            row_Latitude = float(row[2])
            row_Longitude = float(row[3])
            if row[1]!=0:
                if( int(row[1])>0)&(int(row[1])<31):
                    color = icon_color[0]
                elif( int(row[1])>30)&(int(row[1])<61):
                    color = icon_color[1]
                elif (int(row[1])>60)&(int(row[1])<81):
                    color = icon_color[2]
                elif int(row[1])>80:
                    color = icon_color[3]
                station_1.add_child(folium.Marker(location=[row_Latitude, row_Longitude],popup=("<b>Device&nbsp;ID:</b>&nbsp;{NAME}<br>""<b>PM2.5:</b>&nbsp;{PM25}<br>")
                                                .format(NAME=row[0],PM25=row[1]),icon=folium.Icon(color=color)))
        except:
            pass
#===================================================================================#
pm_data = pd.read_csv('/home/s05351035/Jupyter/data_airbox.csv')
bound = taiwanmap_1x1.bounds.copy()
bound['index'] = bound.index
bound['PM2.5'] = [[] for _ in range(len(bound))]
for idx,pm_v in pm_data.iterrows():
    if pm_v['PM2.5']<=0:
        continue
    # use dataframe to find bound information and PM2.5 data Lon and Lat bound
    # get ID that is 符合在界線內的
    grid_id = bound[(bound['minx']<pm_v['Longitude']) & (bound['maxx']>pm_v['Longitude']) & (bound['miny']<pm_v['Latitude']) & (bound['maxy']>pm_v['Latitude'])].index
    if len(grid_id)>0:
        bound.loc[grid_id[0],'PM2.5'].append(pm_v['PM2.5']) #防止一個測站對應到多個網格
    else:
        pass

bound['PM2.5']=bound['PM2.5'].apply(lambda x: 0 if len(x)==0 else round(sum(x)/len(x)))
#===================================================================================#

#建立擴散所需的dataframe
data_idw = pd.DataFrame(columns=['Latitude', 'Longitude', 'PM2.5'])
data_idw['Latitude'] = taiwanmap_1x1['lat']
data_idw['Longitude'] = taiwanmap_1x1['lon']
data_idw['PM2.5'] = bound['PM2.5'].astype('int')
#data_tmp['Id'] = bound['index']

#儲存已有及擴散後的pm2.5值
data_idw_final = pd.DataFrame(columns=['PM2.5' , 'Id'])
data_idw_final['Id'] = bound['index']

#將有資料的網格擷取出來
drop_id = data_idw[(data_idw['PM2.5']==0)].index
data_idw=data_idw.drop(drop_id, axis = 0)

#===================================================================================#
def idw(lat , lon , ref_point):

    total_out = 0
    total_dis = 0
    size_x = 0.00985
    size_y = 0.00904
    sort_df = pd.DataFrame(columns=['Latitude', 'Longitude', 'PM2.5' , 'distance'])

    sort_df['Latitude']=(((ref_point['Latitude']-lat)/size_y)**2)
    sort_df['Longitude']=(((ref_point['Longitude']-lon)/size_x)**2)
    sort_df['PM2.5']=ref_point['PM2.5']
    sort_df['distance']=np.sqrt(sort_df['Latitude']+sort_df['Longitude'])
    sort_df= sort_df.sort_values(by=['distance'])
    sort_df=sort_df.reset_index()
    
    if sort_df.loc[0,'distance']==0:
        return sort_df.loc[0,'PM2.5']
    
    sort_df['distance']=(1/sort_df['distance'])
    
    sort_3 = sort_df[sort_df['distance']>0.33]
    if len(sort_3)==0:
        sort_3 = sort_df[sort_df['distance']>0.2]
    total_dis = sum(sort_3['distance'])

    total_out=round(sum((sort_3['distance']/total_dis)*sort_3['PM2.5']))
    return total_out 
#===================================================================================#
lon = taiwanmap_1x1['lon']
lat = taiwanmap_1x1['lat']

data_final = pd.DataFrame(columns=['Latitude', 'Longitude', 'PM2.5' , 'Id'])
site_name_count = 0

for i,j in zip(lat,lon):
    site_name = site_name_count
    site_name_count += 1
    data_tmp = pd.DataFrame([[i , j , idw(i , j , data_idw) , site_name]] ,columns=['Latitude', 'Longitude', 'PM2.5' , 'Id'])
    data_final = data_final.append(data_tmp)
#===================================================================================#    
data_final=data_final.drop('Latitude', axis = 1)
data_final=data_final.drop('Longitude', axis = 1)
taiwanmap_1x1_final = taiwanmap_1x1.copy()
taiwanmap_1x1_final = taiwanmap_1x1.merge(data_final,on='Id')
#taiwanmap_1x1_final['PM2.5']=taiwanmap_1x1_final['PM2.5'].round()
remove = taiwanmap_1x1_final[(taiwanmap_1x1_final['PM2.5']==0)].index
taiwanmap_1x1_final=taiwanmap_1x1_final.drop(remove, axis = 0)
data_final.to_csv('/var/www/html/data_final.csv',index=False)
#===================================================================================#  
#colorList = ['#98fb98','#00ff00','#32cd32','#ffff00','#ffd700','#ffa500','#ff6347','#ff0000','#ba55d3']
#map_color = cm.StepColormap(colorList,index=[0,10,20,30,40,50,60,70,80],vmin=0,vmax=90,caption = 'PM2.5')
map_color = cm.LinearColormap(colors=['#00DD00','yellow','#FF4500','red','purple','purple'],index=[0,30,60,90,120,150],vmin=0,vmax=150,caption = 'PM2.5(μg/m3)')

fmap = folium.Map(
    (23.697457,120.97),
    zoom_start=8.0,
    min_zoom=8.0,
    min_lat=21, 
    max_lat=27, 
    min_lon=117, 
    max_lon=125,
    max_bounds=True,
)

folium.GeoJson(

                taiwanmap_1x1_final,  
                name='Grid',
                #show = False,
                style_function=lambda x: {
                        'fillColor':map_color(x['properties']['PM2.5']),
                        'color': 'black',
                        'weight': 0,
                        'fillOpacity': 0.7,
                                        },
               highlight_function=lambda x: {'weight':3, 'color':'black'},
               tooltip=folium.GeoJsonTooltip(fields=['PM2.5','lat','lon'],aliases=['PM2.5','lat','lon'],labels=True,sticky=True)
             

                ).add_to(fmap)

fmap.add_child(station_1)
fmap.add_child(map_color)
folium.LayerControl().add_to(fmap)
fmap.save('/var/www/html/map_airbox.html')
#====================================================================================#
fmap2 = folium.Map(
    (24.1788106,120.6042912),
    zoom_start=10.5,
    min_zoom=8.0,
    min_lat=21, 
    max_lat=27, 
    min_lon=117, 
    max_lon=125,
    max_bounds=True,
)

folium.GeoJson(

                taiwanmap_1x1_final,  
                name='Grid',
                #show = False,
                style_function=lambda x: {
                        'fillColor':map_color(x['properties']['PM2.5']),
                        'color': 'black',
                        'weight': 0,
                        'fillOpacity': 0.7,
                                        },
               highlight_function=lambda x: {'weight':3, 'color':'black'},
               tooltip=folium.GeoJsonTooltip(fields=['PM2.5'],aliases=['PM2.5'],labels=True,sticky=True)
             

                ).add_to(fmap2)

fmap2.add_child(station_1)
fmap2.add_child(map_color)
folium.LayerControl().add_to(fmap2)

fmap2.save('/var/www/html/map_airbox_taichung.html')
#====================================================================================#
#彰化
fmap3 = folium.Map(
    (23.966666666666665,120.46666666666667),
    zoom_start=10.5,
    min_zoom=8.0,
    min_lat=21, 
    max_lat=27, 
    min_lon=117, 
    max_lon=125,
    max_bounds=True,
)

folium.GeoJson(

                taiwanmap_1x1_final,  
                name='Grid',
                #show = False,
                style_function=lambda x: {
                        'fillColor':map_color(x['properties']['PM2.5']),
                        'color': 'black',
                        'weight': 0,
                        'fillOpacity': 0.7,
                                        },
               highlight_function=lambda x: {'weight':3, 'color':'black'},
               tooltip=folium.GeoJsonTooltip(fields=['PM2.5'],aliases=['PM2.5'],labels=True,sticky=True)
             

                ).add_to(fmap3)

fmap3.add_child(station_1)
fmap3.add_child(map_color)
folium.LayerControl().add_to(fmap3)

fmap3.save('/var/www/html/map_airbox_ch.html')
#====================================================================================#
#刪除變數,釋放記憶體
import gc

del taiwanmap_1x1_final
del data_idw
del taiwanmap_1x1
del data_final
del bound

gc.collect()
#====================================================================================#
#截圖


from selenium import webdriver


update = str(datetime.datetime.now())
year=update[:4]
month=update[5:7]
day=update[8:10]
hour=update[11:13]
minute=int(update[14:16])

if minute<30:
    name='/var/www/html/history_airbox/'+year+month+day+hour+'00.png'
else:
    name='/var/www/html/history_airbox/'+year+month+day+hour+'30.png'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1280,720')
client = webdriver.Chrome(options=chrome_options, executable_path='/var/www/html/chromedriver') 
 
client.get("http://140.128.98.82/history_airbox.php")
#client.maximize_window()
client.refresh()
time.sleep(10)
client.save_screenshot(name)
time.sleep(5)
client.quit()
#=====================================#
#GIF
import imageio

img_paths=[]
for i in range(49):
    
    past = datetime.datetime.now()+datetime.timedelta(minutes=-i*30)
    str_past = str(past)
    hour = str_past[11:13]
    year = str_past[:4]
    month=str_past[5:7]
    day=str_past[8:10]
    minute_temp=int(str_past[14:16])
    if minute_temp <30:
        minute = '00'
    else:
        minute = '30'
    name='/var/www/html/history_airbox/'+year+month+day+hour+minute+'.png'
    img_paths.insert(0,name)

gif_images = []
for path in img_paths:
    try:
        gif_images.append(imageio.imread(path))
    except:
        pass
imageio.mimsave("/var/www/html/history_airbox.gif",gif_images,fps=0.8)
