from csvsort import csvsort
import pandas as pd
import csv
from datetime import datetime
import osmnx as ox
import osrm as osrm
from reportlab.pdfbase.acroform import annotationFlagValues
from pymongo import MongoClient
from bson.int64 import long

client = osrm.Client(host='http://osrm-1644136849.us-east-1.elb.amazonaws.com')
csv_file="traffic-data-2.csv"
G = ox.graph_from_bbox(12.867853, 12.842623,  77.803543, 77.775726, network_type='drive')
rider_last_value={}
conn=MongoClient('localhost', 27017)
collection=conn.config.geo_spatial

def find_nearest_node(coordinate):
    geom,u,v=ox.get_nearest_edge(G, (coordinate['lat'],coordinate['lng']))    
    return u

def calc_distance_between_nodes(nodeA,nodeB,total_time,time_entered):
    response=client.route(coordinates=[[G.node[nodeA]['x'],G.node[nodeA]['y']],[G.node[nodeB]['x'],G.node[nodeB]['y']]],annotations=True);
    
    nodes_path=response['routes'][0]['legs'][0]['annotation']['nodes']
    
    time_entered=time_entered[0:13]
    time_entered_datetime=datetime.strptime(time_entered,'%Y-%m-%d %H')
    
    if(nodeB not in nodes_path):
        nodes_path.str(nodeB)
    total_distance=0
    current_node=nodeA
    for node in nodes_path:
        
        if node==current_node:
            continue
        total_distance=total_distance+ox.utils.euclidean_dist_vec(G.nodes[node]['y'], G.nodes[node]['x'], G.nodes[current_node]['y'], G.nodes[current_node]['x'])
        current_node=node
        
    speed=float(total_distance)/total_time
    current_node=nodeA
    for node in nodes_path:
        
        if node==current_node:
            continue
        
        current_distance=ox.utils.euclidean_dist_vec(G.nodes[node]['y'], G.nodes[node]['x'], G.nodes[current_node]['y'], G.nodes[current_node]['x'])
        final_query={
            'node_from':str(current_node),
            'node_to': str(node),
            'time.hour':str(time_entered_datetime.hour),
            'time.date':str(time_entered_datetime.day),
            'time.month':str(time_entered_datetime.month),
            'time.year':str(time_entered_datetime.year),
            'date':time_entered_datetime
            }
        final_set_doc={
            'distance': current_distance,
            'time.day':time_entered_datetime.strftime('%A')
            }
        final_inc_doc={
            'total_time_taken':long(current_distance/speed),
            'number_of_data_points':1
            }
        
        collection.update(final_query,{"$set":final_set_doc,"$inc":final_inc_doc},upsert=True)

with open(csv_file) as csv_file:
    csv_reader=csv.reader(csv_file,delimiter=',')
    line=0
    for row in csv_reader:
        if(line>0):
            user_id=row[0]
            lat=row[1]
            lng=row[2]
            time=datetime.strptime(row[3][0:19],'%Y-%m-%d %H:%M:%S')
            
            coordinate={}
            coordinate['lat']=float(lat)
            coordinate['lng']=float(lng)
            
            if(user_id not in rider_last_value):
                rider_last_value[user_id]={'coordinate':coordinate,'time':time}
            else:
                total_time=(time-rider_last_value[user_id]['time']).total_seconds()
                if(total_time>1800):
                    del rider_last_value[user_id]
                    continue
                
                startNode=find_nearest_node(rider_last_value[user_id]['coordinate'])
                endNode=find_nearest_node(coordinate)
                calc_distance_between_nodes(startNode,endNode,total_time,row[3])
        line=line+1

print('done')