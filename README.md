## Environment Variables

* `ENABLE_LEMETHOD`: Whether to enable `LeMethod`, non-zero to enable.
* `DMLC_RANK`: The rank of the current node, starting from 0.
You should make sure they are unique and continuous.
* `LEMETHOD_CONNECTION_TYPE`: The connection type for `LeMethod`.
  * `0`: This is like the original PS-lite connection type.
  In this type, each worker node can communicate with the server node
  but not with any other worker nodes.
  * `1`: This is a complete connection type.
  In this type, each node can communicate with all other nodes,
  including both worker nodes and server nodes.
  * `2`: This is a user-defined connection type.
  This is an extension of the original PS-lite connection type. In this type,
  users can add some additional links between worker nodes.
* `LEMETHOD_CONF_PATH`: The path to the configuration file for `LeMethod`.
Every line in the file should be one of the following formats (empty lines are ignored):
  * `ADD_CONNECTION <node_a> <node_b>`: Only valid in connection type 2.
  This adds a connection between node_a and node_b,
  and just allows `node_a` to send data to `node_b`.
  * `SET_SCHEDULE_RATIO <ratio>`: Schedule ratio is a float number in the range [0, 1].
  This is used to decided how many nodes to schedule in each iteration of aggregation.
  * `SET_SCHEDULE_NUM <num>`: The number of nodes to schedule
  in each iteration of model aggregation.
  This variable has a higher priority than `SET_SCHEDULE_RATIO`.
  * `SET_BANDWIDTH_EXPIRATION <num>`: The expiration iteration for bandwidth.
  This is used to decide how many iterations the bandwidth will be valid.
* `GREED_RATE`: The greed rate for `LeMethod`, a float number in the range [0, 1].
The greed rate is used to decide the probability of a node to send data to the best receiver.

## LeMethod Protocol

New Message Type:

* `ASK_LOCAL_AGGREGATION`: Worker nodes use this message to ask the scheduler node
which node should be the receiver of the local aggregation.
* `ASK_AS_RECEIVER`: When the scheduler node find a receiver for the local aggregation,
it will send this message to the receiver node to ask whether it can receive the local aggregation.
* `ASK_AS_RECEIVER_REPLY`: When the receiver node receives the `ASK_AS_RECEIVER` message,
it will reply with this message to the scheduler node to indicate
whether it can receive the local aggregation.
* `ASK_LOCAL_AGGREGATION_REPLY`: The scheduler node replies to the worker nodes
with the node that should be the receiver of the local aggregation.
* `LOCAL_AGGREGATION`: One node sends his local data to the receiver node
to start the local aggregation.
* `FINISH_RECEIVING_LOCAL_AGGREGATION`: When the receiver node finishes receiving
the local aggregation data, it will send this message to the scheduler node to notify that
the local aggregation is finished.
* `NOTIFY_WORKER_ONE_ITERATION_FINISH`: When the server node receives
all the local aggregation data, it will notify all worker nodes with this message
that the local aggregation is finished so that they can update some metadata.
* `ASK_MODEL_RECEIVER`: Once a node received the new model,
it will send this message to the scheduler node to ask which node should be the receiver
of the model distribution.
* `ASK_MODEL_RECEIVER_REPLY`: The scheduler node replies to the worker nodes
with the node that should be the receiver of the model distribution.
* `MODEL_DISTRIBUTION`: One node sends the new model to the receiver node
the new model data.
* `MODEL_DISTRIBUTION_REPLY`: The receiver node replies to the sender node
that it has received the new model data.

[![Build Status](https://travis-ci.org/dmlc/ps-lite.svg?branch=master)](https://travis-ci.org/dmlc/ps-lite)
[![GitHub license](http://dmlc.github.io/img/apache2.svg)](./LICENSE)

A light and efficient implementation of the parameter server
framework. It provides clean yet powerful APIs. For example, a worker node can
communicate with the server nodes by
- `Push(keys, values)`: push a list of (key, value) pairs to the server nodes
- `Pull(keys)`: pull the values from servers for a list of keys
- `Wait`: wait untill a push or pull finished.

A simple example:

```c++
  std::vector<uint64_t> key = {1, 3, 5};
  std::vector<float> val = {1, 1, 1};
  std::vector<float> recv_val;
  ps::KVWorker<float> w;
  w.Wait(w.Push(key, val));
  w.Wait(w.Pull(key, &recv_val));
```

More features:

- Flexible and high-performance communication: zero-copy push/pull, supporting
  dynamic length values, user-defined filters for communication compression
- Server-side programming: supporting user-defined handles on server nodes

### Build

`ps-lite` requires a C++11 compiler such as `g++ >= 4.8`. On Ubuntu >= 13.10, we
can install it by
```
sudo apt-get update && sudo apt-get install -y build-essential git
```
Instructions for gcc 4.8 installation on other platforms:
- [Ubuntu 12.04](http://ubuntuhandbook.org/index.php/2013/08/install-gcc-4-8-via-ppa-in-ubuntu-12-04-13-04/)
- [Centos](http://linux.web.cern.ch/linux/devtoolset/)
- [Mac Os X](http://hpc.sourceforge.net/).

Then clone and build

```bash
git clone https://github.com/dmlc/ps-lite
cd ps-lite && make -j4
```

### How to use

`ps-lite` provides asynchronous communication for other projects: 
  - Distributed deep neural networks:
    [MXNet](https://github.com/dmlc/mxnet),
    [CXXNET](https://github.com/dmlc/cxxnet),
    [Minverva](https://github.com/minerva-developers/minerva), and
    [BytePS](https://github.com/bytedance/byteps/)
  - Distributed high dimensional inference, such as sparse logistic regression,
    factorization machines:
    [DiFacto](https://github.com/dmlc/difacto)
    [Wormhole](https://github.com/dmlc/wormhole)


### Research papers
  1. Mu Li, Dave Andersen, Alex Smola, Junwoo Park, Amr Ahmed, Vanja Josifovski,
     James Long, Eugene Shekita, Bor-Yiing
     Su. [Scaling Distributed Machine Learning with the Parameter Server](http://www.cs.cmu.edu/~muli/file/parameter_server_osdi14.pdf). In
     Operating Systems Design and Implementation (OSDI), 2014
  2. Mu Li, Dave Andersen, Alex Smola, and Kai
     Yu. [Communication Efficient Distributed Machine Learning with the Parameter Server](http://www.cs.cmu.edu/~muli/file/parameter_server_nips14.pdf). In
     Neural Information Processing Systems (NIPS), 2014
