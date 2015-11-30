# Dijkstra-Shortest-Path


Shortest Paths in Graphs
Let's have a look at the problem of finding the shortest path in a graph. To start with, this graph:

![alt tag](http://tinypic.com/r/2z8dzbd/9)

We will represent the graph by listing the outgoing edges of each node (and all of our nodes will be labelled with integers):

1: 3 5
2: 4
3: 2 5
4: 3
5:
6: 1 5
If we are asked to find the shortest path from 1 to 4, we should return 1, 3, 2, 4.
