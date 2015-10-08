'''
Created on Oct 7, 2015

@author: MatthewRubashkin
'''
import matplotlib.pyplot as plt
from scipy.cluster.vq import *
import numpy as np
import json, os, math


dir = 'data/'
query = 'data'
filename = 'analysis_' + query + '_max_nodes_test_151001_test_url_ranks_links_23.json'

#lists for graphing
url=[]
rank=[]
total_links=[]
rank_and_links=[] # used for graphing

#open file and append to lists for graphing
def open_parse_save_analysis_file(dir):
    for filename in  os.listdir(dir):
        #print filename
        if 'analysis_data' in filename:
            with open(dir+filename, 'r') as f:
                json_file = ((f.readlines()))
                dict_array=json.loads(json_file[0])
                for dict_object in dict_array:
                    #print dict_object
                    url.append(dict_object['url'])
                    rank.append(dict_object['rank'])
                    total_links.append(dict_object['total_links'])
                    rank_and_links.append([dict_object['total_links'], dict_object['rank']])
                    #take the log10 of rank to normalize
                    #rank_and_links.append([dict_object['total_links'], math.log10(dict_object['rank'])])
                    

open_parse_save_analysis_file(dir)

#Convert the lists to np arrays
print len(url)
url_np=np.array(url)
rank_np=np.array(rank)
total_links_np=np.array(total_links)  
rank_and_links_np=np.array(rank_and_links)

print rank_and_links_np

    
#Make graph
def simple_graph():
    fig = plt.figure()
    ax = plt.gca()      
    ax.scatter(total_links_np, rank_np,  c='magenta', alpha=0.75, edgecolors='none')
    ax.set_yscale('log')
    plt.show() 

def cluster_graph(xy):
    # kmeans for 4 clusters
    res, idx = kmeans2(np.array(zip(xy[:,0],xy[:,1])),4,iter=100)
    #res, idx = kmeans2(np.array(zip(xy[:,0],math.log10(xy[:,1]))),4)
    
     
    colors = ([([0.4,1,0.4],[1,0.4,0.4],[0.1,0.8,1],[1.0,.2,1.0])[i] for i in idx])
    
    # plot colored points
    
     
    # mark centroids as (X)
    fig = plt.figure(facecolor='white')
    ax = plt.gca() 
    ax.scatter(res[:,0],res[:,1], marker='o', s = 500, linewidths=2, c='none')
    ax.scatter(res[:,0],res[:,1], marker='x', s = 500, linewidths=2)
    ax.scatter(xy[:,0],xy[:,1], c=colors)
    ax.set_yscale('log')
    #label plot
    plt.xlabel('Hyperlinks')
    plt.ylabel('Global PageRank')
    plt.title('KMeans Cluster Analysis: Query=Data')
    plt.grid(True)
    plt.ylim([2,500]) 
    plt.xlim([2,500])
    plt.show()
    fig.savefig('/Users/MatthewRubashkin/Desktop/kmeans.pdf',bbox_inches='tight')


cluster_graph(rank_and_links_np)




if __name__ == '__main__':
    pass
