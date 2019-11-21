# Hadoop_Mapreduce_BerkleyGraphDataset

Assignment:

Introduction:
1. Dataset is of directed graph.
2. Data location: /data/graph
3. Sample Data : data/graph/sample.txt
4. There might be noise in the dataset.
5. Line in the data file starting with # should be ignored.
6. Do not include sample.txt in the final processing.

Task:
1. Read the directed graph data, make it undirected.
2. Create adjacency list for the directed graph.
3. Create adjacency list for the undirected graph.
4. Find the longest adjacency list in directed and undirected graph.
5. Find the node with maximum and minimum connectivity.

Dataset:
https://snap.stanford.edu/data/web-BerkStan.html

Hadoop commands used:

(compile java file)
hadoop com.sun.tools.javac.Main adjList.java -d Sreenath

(creating jar file)
jar -cvf adjList.jar -C Sreenath/ .

(Listing the files in the directory)
hadoop fs -ls /data/graph/

(Removing the files or directories)
hadoop fs -rm -r /tmp/pabbatph/undirectedresult
hadoop fs -rm -r /tmp/pabbatph/directedresult

(run the hadoop program)
hadoop jar adjList.jar adjList /data/graph/web-BerkStan.txt /tmp/pabbatph/undirectedresult /tmp/pabbatph/directedresult

(output command) 
hadoop fs -cat /tmp/pabbatph/undirectedresult/part-r-00000
hadoop fs -cat /tmp/pabbatph/directedresult/part-r-00000
