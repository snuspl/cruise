# Cruise PS: A Parameter Server implementation in Cruise

The key characteristic of Cruise PS is cost-based optimization of system configuration, which is defined as the number of servers (S), the number of workers (W), the distribution of training data (D), and the distribution of model data (M). Cruise PS collects metrics related to the performance from a running job, and the Optimizer captures the relationship between the system configuration and the performance (in terms of running time).
After the (near-)optimal configuration is computed, the configuration is changed at runtime with the support of [Elastic Tables (ET)](https://github.com/snuspl/cay/tree/master/services/et), which provides a common abstraction for data and containers - with the primitive operations that ET provides, Cruise PS is able to change the configuration without an adverse effect to running ML jobs.
You can find a more detailed description in our paper (ICML 2016 MLSys Workshop).


## How to run applications?

On Cruise PS, we have implemented 7 applications:
* Classification: Non-negative Matrix Factorization (NMF)
* Recommendation: Multinomial Logistic Regression (MLR)
* Topic modeling: Latent Dirichlet Allocation (LDA)
* Decision/Regression tree: Gradient Boosting Tree
* Feature selection: Lasso
* (for testing) Add integer and Add vector

You can find shell script files to run those appliations in `bin` directory. Each script consists of example usage to inform you which parameters are configurable. For example, `run_nmf.sh` looks as follows:
```
...
# EXAMPLE USAGE
# ./run_nmf.sh -max_num_epochs 50 -local true -number_servers 1 -num_workers 4 -max_num_eval_local 5 -input sample_nmf -num_mini_batches 25 -num_worker_blocks 25 -rank 30 -step_size 0.01 -print_mat true -timeout 300000 -decay_period 5 -decay_rate 0.9 -optimizer edu.snu.cay.dolphin.async.optimizer.impl.EmptyPlanOptimizer -optimization_interval_ms 3000 -delay_after_optimization_ms 10000 -opt_benefit_threshold 0.1 -server_metric_flush_period_ms 1000 -moving_avg_window_size 0 -metric_weight_factor 0.0 -num_initial_batch_metrics_to_skip 3 -min_num_required_batch_metrics 3 -hyper_thread_enabled false
...
```

Copy-and-paste the command to the terminal (e.g., bash), then the application will run.

## Dashboard

- Cruise PS provides a dashboard service which visualizes the procedure of cruise applications.
- Dashboard service can be activated by adding `-dashboard {port number for the dashboard}`.
- A dashboard server(flask) will be established on the client machine and other machines can access
  the visualized data by connecting to `http://{host address of the client machine}:{port number}`
- The port number should be an integer between 0 and 66636 which has not been already used. If the
  application fails to bind the port, it will disable the server automatically.
  
##### Requirements
  - Flask(python): `sudo pip install Flask`.
