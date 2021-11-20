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
import base64
from matplotlib import pyplot as plt
# ============================================#
requests.packages.urllib3.disable_warnings()
start = datetime.datetime.now()
# 環保署微型感測器爬蟲
row=0
data = pd.DataFrame(columns = ['ID','PM2.5','source'])
url=['https://sta.ci.taiwan.gov.tw/STA_AirQuality_EPAIoT/v1.0/Datastreams?$expand=Thing,Observations($orderby=phenomenonTime%20desc;$top=1)&$filter=name%20eq%20%27PM2.5%27&$count=true']
for j in range(100):
    res_2 = requests.get(url[j], verify=False)
    row_data_2 = json.loads(res_2.text)
    try:
        url.append(row_data_2['@iot.nextLink'])
    except:
        pass
    for i in range(len(row_data_2['value'])):
        data.loc[row,'ID'] = row_data_2['value'][i]['Thing']['properties']['stationID']
        try:
            data.loc[row,'PM2.5'] = int(row_data_2['value'][i]['Observations'][0]['result'])
        except:
            data.loc[row,'PM2.5']=0
        row = row + 1
    if url[j]==url[-1]:
        break
csv_data = pd.read_csv('/home/s05351035/Jupyter/EPAIoT_station.csv', names = ['ID','Latitude','Longitude'])
csv_data = pd.DataFrame(csv_data).astype('str')
data = data.merge(csv_data,on='ID')
data['source']=0
num_0 = len(data[data['PM2.5']!=0])
#============================================#
#國家測站


csv_data2 = pd.read_csv('/home/s05351035/Jupyter/EPA77.csv')

res = requests.get('https://sta.ci.taiwan.gov.tw/STA_AirQuality_v2/v1.0/Datastreams?$expand=Thing,Observations($orderby=phenomenonTime%20desc;$top=1)&$filter=name%20eq%20%27PM2.5%27%20and%20Thing/properties/authority%20eq%20%27%E8%A1%8C%E6%94%BF%E9%99%A2%E7%92%B0%E5%A2%83%E4%BF%9D%E8%AD%B7%E7%BD%B2%27%20and%20substringof(%27%E7%A9%BA%E6%B0%A3%E5%93%81%E8%B3%AA%E6%B8%AC%E7%AB%99%27,Thing/name)&$count=true', verify=False)
row_data = json.loads(res.text)
for i in range(len(row_data['value'])):
    stationName = row_data['value'][i]['Thing']['properties']['stationName']
    index = csv_data2[csv_data2['stationName']==stationName].index
    try:
        result = int(row_data['value'][i]['Observations'][0]['result'])
        csv_data2.loc[index,'PM2.5'] = result
    except:
        csv_data2.loc[index,'PM2.5'] = 0
csv_data2['source']= 1
csv_data2 = csv_data2.drop('stationName',axis=1)
num_1 = len(csv_data2[csv_data2['PM2.5']!=0])


#============================================#
# 大同微型感測器
row=0
data_dt = pd.DataFrame(columns = ['ID','PM2.5','source'])
url=['https://sta.ci.taiwan.gov.tw/STA_AirQuality_Tatung/v1.0/Datastreams?$expand=Thing,Observations($orderby=phenomenonTime%20desc;$top=1)&$count=true']
for j in range(100):
    res = requests.get(url[j], verify=False)
    row_data = json.loads(res.text)
    try:
        url.append(row_data['@iot.nextLink'])
    except:
        pass
    for i in range(len(row_data['value'])):
        data_dt.loc[row,'ID'] = row_data['value'][i]['Thing']['properties']['stationID']
        try:
            result = int(row_data['value'][i]['Observations'][0]['result'])
            if result<=150:
                data_dt.loc[row,'PM2.5'] = result
            else:
                data_dt.loc[row,'PM2.5'] = 0
        except:
            data_dt.loc[row,'PM2.5']=0
        row = row + 1
    if url[j]==url[-1]:
        break
# 與感測器位置合併
dt_station = pd.read_csv('/home/s05351035/Jupyter/dt_station.csv', names = ['ID','Latitude','Longitude'])
dt_station = pd.DataFrame(dt_station).astype('str')
data_dt = data_dt.merge(dt_station,on='ID')
data_dt['source']=2
num_2 = len(data_dt[data_dt['PM2.5']!=0])

#============================================#
# 暨南大學感測器
row=0
data_local = pd.DataFrame(columns = ['ID','PM2.5','source'])
url=['https://sta.ci.taiwan.gov.tw/STA_AirQuality_Local/v1.0/Datastreams?$expand=Thing,Observations($orderby=phenomenonTime%20desc;$top=1)&$count=true']
for j in range(100):
    res = requests.get(url[j], verify=False)
    row_data = json.loads(res.text)
    try:
        url.append(row_data['@iot.nextLink'])
    except:
        pass
    for i in range(len(row_data['value'])):
        data_local.loc[row,'ID'] = row_data['value'][i]['Thing']['properties']['stationID']
        try:
            data_local.loc[row,'PM2.5'] = int(row_data['value'][i]['Observations'][0]['result'])
        except:
            data_local.loc[row,'PM2.5']=0
        row = row + 1
    if url[j]==url[-1]:
        break
local_station = pd.read_csv('/home/s05351035/Jupyter/local_station.csv', names = ['ID','Latitude','Longitude'])
local_station = pd.DataFrame(local_station).astype('str')
data_local = data_local.merge(local_station,on='ID')
data_local['source']=3
num_3 = len(data_local[data_local['PM2.5']!=0])

#============================================#
#合併資料
# csv_data = 環保署微型感測器
# csv_data2 = 國家空品測站
# data_dt = 大同微型感測器
data=data.append(csv_data2,sort=True)
data=data.append(data_dt,sort=True)
data=data.append(data_local,sort=True)
data=data.reset_index(drop=True)
# ============================================#
#存成csv檔
data.to_csv('/home/s05351035/Jupyter/data_epa.csv',index=False)
#===================================================================================#
#建立網格地圖
sys.setrecursionlimit(1000000)
taiwanmap_1x1 = gpd.read_file("/home/s05351035/Jupyter/Taiwan_1x1_map/Taiwan_1x1_map.shp",encoding='utf-8')
taiwanmap_1x1.crs = {'init' :'epsg:3826'}
taiwanmap_1x1=taiwanmap_1x1.to_crs(epsg=4326)
taiwanmap_1x1=taiwanmap_1x1.reset_index()
#===================================================================================#

#地圖打點           
icon_color=['green','orange','red','purple']

name_0 = 'Micro-Sensor('+str(num_0)+')'
name_1 = 'Station('+str(num_1)+')'
name_2 = 'Tatung-Sensor('+str(num_2)+')'
name_3 = 'Chi Nan-Sensor('+str(num_3)+')'
station_0=folium.FeatureGroup(name=name_0,show = False)
station_1=folium.FeatureGroup(name=name_1,show = False)
station_2=folium.FeatureGroup(name=name_2,show = False)
station_3=folium.FeatureGroup(name=name_3,show = False)

d = pd.read_csv('/home/s05351035/Jupyter/data_epa.csv')  
d = d[d['PM2.5']!=0]
d = d.reset_index(drop=True)
for i in range(len(d)):
    s = d.loc[i,'source']
    lat = d.loc[i,'Latitude']
    lon = d.loc[i,'Longitude']
    p = d.loc[i,'PM2.5']
    Id = d.loc[i,'ID']
    if(p>0)&(p<31):
        color = icon_color[0]
    elif(p>30)&(p<61):
        color = icon_color[1]
    elif (p>60)&(p<81):
        color = icon_color[2]
    elif p>80:
        color = icon_color[3]
    if(s==0):
        station_0.add_child(folium.Marker(location=[lat, lon],popup=("<b>Device&nbsp;ID:</b> {NAME}<br>""<b>PM2.5:</b>{PM25}<br>").format(NAME=Id,PM25=p),icon=folium.Icon(color=color)))
    elif(s==1):
        station_1.add_child(folium.Marker(location=[lat, lon],popup=("<b>Device&nbsp;ID:</b> {NAME}<br>""<b>PM2.5:</b>{PM25}<br>").format(NAME=Id,PM25=p),icon=folium.Icon(color=color)))
    elif(s==2):
        station_2.add_child(folium.Marker(location=[lat, lon],popup=("<b>Device&nbsp;ID:</b> {NAME}<br>""<b>PM2.5:</b>{PM25}<br>").format(NAME=Id,PM25=p),icon=folium.Icon(color=color)))
    elif(s==3):
        station_3.add_child(folium.Marker(location=[lat, lon],popup=("<b>Device&nbsp;ID:</b> {NAME}<br>""<b>PM2.5:</b>{PM25}<br>").format(NAME=Id,PM25=p),icon=folium.Icon(color=color)))

   
del d
# ============================================#
pm_data = pd.read_csv('/home/s05351035/Jupyter/data_epa.csv')
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
data_idw['PM2.5'] = bound['PM2.5'].astype('float')
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
remove = taiwanmap_1x1_final[(taiwanmap_1x1_final['PM2.5']==0)].index
taiwanmap_1x1_final=taiwanmap_1x1_final.drop(remove, axis = 0)
#===================================================================================#  
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
                style_function=lambda x: {
                        'fillColor':map_color(x['properties']['PM2.5']),
                        'color': 'black',
                        'weight': 0,
                        'fillOpacity': 0.7,
                                        },
               highlight_function=lambda x: {'weight':3, 'color':'black'},
               tooltip=folium.GeoJsonTooltip(fields=['PM2.5','lat','lon'],aliases=['PM2.5','lat','lon'],labels=True,sticky=True)
             

                ).add_to(fmap)

fmap.add_child(station_0)
fmap.add_child(station_1)
fmap.add_child(station_2)
fmap.add_child(station_3)
fmap.add_child(map_color)
folium.LayerControl().add_to(fmap)

fmap.save('/var/www/html/map_epa.html')
#===================================================================================#
# 台中
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
                        'weight':0,
                        'fillOpacity': 0.7,
                                        },
               highlight_function=lambda x: {'weight':3, 'color':'black'},
               tooltip=folium.GeoJsonTooltip(fields=['PM2.5'],aliases=['PM2.5'],labels=True,sticky=True)
             

                ).add_to(fmap2)

fmap2.add_child(station_0)
fmap2.add_child(station_1)
fmap2.add_child(station_2)
fmap2.add_child(station_3)
fmap2.add_child(map_color)
folium.LayerControl().add_to(fmap2)

fmap2.save('/var/www/html/map_epa_taichung.html')
#===================================================================================#
# 彰化
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

fmap3.add_child(station_0)
fmap3.add_child(station_1)
fmap3.add_child(station_2)
fmap3.add_child(station_3)
fmap3.add_child(map_color)
folium.LayerControl().add_to(fmap3)

fmap3.save('/var/www/html/map_epa_ch.html')
#===================================================================================#
#刪除變數,釋放記憶體
import gc

del taiwanmap_1x1_final
del data_idw
del taiwanmap_1x1
del data_final
del bound

gc.collect()
#===================================================================================#
#截圖


from selenium import webdriver

update = str(datetime.datetime.now())
year=update[:4]
month=update[5:7]
day=update[8:10]
hour=update[11:13]
minute=int(update[14:16])
if minute<30:
    name='/var/www/html/history_epa/'+year+month+day+hour+'00.png'
else:
    name='/var/www/html/history_epa/'+year+month+day+hour+'30.png'
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1280,720')
client = webdriver.Chrome(options=chrome_options, executable_path='/var/www/html/chromedriver') 
 
client.get("http://140.128.98.82/history_epa.php")
#client.maximize_window()
client.refresh()
time.sleep(10)
client.save_screenshot(name)
time.sleep(5)
client.quit()

#=======================================================#
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
    name='/var/www/html/history_epa/'+year+month+day+hour+minute+'.png'
    img_paths.insert(0,name)

gif_images = []
for path in img_paths:
    try:
        gif_images.append(imageio.imread(path))
    except:
        pass
imageio.mimsave("/var/www/html/history_epa.gif",gif_images,fps=0.8)
