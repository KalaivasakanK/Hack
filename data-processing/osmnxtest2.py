import osmnx as ox
G = ox.graph_from_bbox(12.867853, 12.842623,  77.803543, 77.775726, network_type='drive')
geom,u,v=ox.get_nearest_edge(G, (12.855777, 77.783582)) #12.85921/77.78350
G_projected = ox.project_graph(G)

print(geom)
print(u)
print(v)
ox.plot_graph(G_projected)