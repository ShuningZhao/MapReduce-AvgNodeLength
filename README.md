# MapReduce-AvgNodeLength
By Shuning Zhao April 2018

Given a directed graph, compute the average length of the in-coming edges for each node.

**Input files:**  
In the input file, each line is in format of:  
“EdgeId FromNodeId ToNodeId Distance”.

![](https://github.com/ShuningZhao/MapReduce-AvgNodeLength/blob/master/Images/01Graph.png?raw=true)

In the above example, the input is like:

![](https://github.com/ShuningZhao/MapReduce-AvgNodeLength/blob/master/Images/02Table.png?raw=true)

Which is the sample file [tiny-graph.txt](https://webcms3.cse.unsw.edu.au/COMP9313/18s1/resources/15769).

**Output:**  
Set the number of reducers to 1. Each line in the single output file is in format of “NodeID\tAverage length of in-coming edges”. The average length is of double precision (using DoubleWritable). Remove the nodes that have no in-coming edges and sort the output by NodeID in ascending order of its numeric value. Given the example graph, the output file is like:

![](https://github.com/ShuningZhao/MapReduce-AvgNodeLength/blob/master/Images/03Results.png?raw=true)

**Files**  
The file [EdgeAvgLen1.java](https://github.com/ShuningZhao/MapReduce-AvgNodeLength/blob/master/EdgeAvgLen1.java) utilizes a combiner and the file [EdgeAvgLen2.java](https://github.com/ShuningZhao/MapReduce-AvgNodeLength/blob/master/EdgeAvgLen2.java) utilizes a in-mapper combining approach to solve this problem.