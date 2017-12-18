# Cruise Pregel: BSP-style graph processing engine
(writing in progress)

- Cruise Pregel processes data with a graph structure (i.e., set of vertices and edges).
- The notion of superstep; which messages are exchanged and which computation are done
  
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

