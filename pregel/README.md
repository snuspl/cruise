# Cruise Pregel: BSP-style graph processing engine
Cruise Pregel processes data represented as a graph structure (i.e., set of vertices and edges) in a synchronous way. We built Cruise Pregel on top of Elastic Table based on [the paper from Google](https://dl.acm.org/citation.cfm?id=1807184). In high-level, Pregel applications work as follows:
1. Each iteration is called `superstep`, where each Task receives messages from the previous superstep if the Task has vertices that are connected with Vertices in other Tasks. 
2. Then Tasks update their local state and prepare the updates to propagate to other Tasks. 
3. Tasks wait until all of them to finish the local computation.
4. Messages are exchanged and start the next superstep.
5. Repeat 1-4 until converged.
  
## How to run applications?

On Cruise Pregel, we have implemented two applications:
  - Pagerank
  - Shortest path

You can find shell script files to run those applications in the `bin` directory. Each script consists of example usage to inform you which parameters are configurable. For example, `run_pagerank.sh` looks as follows:
```
# EXAMPLE USAGE
# ./run_pagerank.sh -local true -max_num_eval_local 3 -num_workers 3 -input file://$(pwd)/inputs/adj_list -worker_mem_size 128 -worker_num_cores 1 -driver_memory 128 -timeout 300000
```

Copy-and-paste the command to the terminal (e.g., bash), then the application will run.

