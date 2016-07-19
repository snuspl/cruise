# Deep Neural Network

This module implements a deep learning framework that is designed for training large neural network models on big data by supporting data partitioning as well as model partitioning, inspired by Google's [DistBelief](http://papers.nips.cc/paper/4687-large-scale-distributed-deep-networks.pdf), although the current codebase only contains methods for data partitioning; model partitioning is on-going work.

* Data partitioning: Input data are distributed across evaluators, each of which has a replica of the whole neural network model. Every replica independently trains its model on its own data, and the updated models are shared between replicas periodically. The model sharing can be done either synchronously or asynchronously, depending on the implementation.

<p align="center"><img src="http://cmslab.snu.ac.kr/home/wp-content/uploads/2015/09/Data-Partitioning.png" alt="Data Partitioning" width="646px" height="410px"/></p>

* Model partitioning: Each partition works on a certain portion of the neural network model. Partitions of a model need to process the same training data at a given time, whereas in data partitioning model replicas make progress without regard to each other.

<p align="center"><img src="http://cmslab.snu.ac.kr/home/wp-content/uploads/2015/09/Model-Partitioning.png" alt="Model Partitioning" width="646px" height="374px"/></p>

## Input file format
This DNN module can process Numpy-compatible plain text input files which are stored in the following format.

<p align="center"><img src="http://cmslab.snu.ac.kr/home/wp-content/uploads/2015/09/Input-Data-Format.png" alt="Input File Format" width="625px" height="202px"/></p>

Each line represents a vector whose elements are separated using a delimiter, specified via the command line parameter [`delim`](#parameter-delim). Vectors should consist of serialized input data and other metadata. We assume that each element can be converted to a floating number `float`.

* *Serialized input data*: an input data object that is serialized as a vector. The shape of input data can be specified in a separate [protocol buffer definition file](#configuration-input_shape).
* *Output*: the expected output for a given input data object.
* *Validation flag*: a flag that indicates whether an input data is used for validation: `1.0` for validating, and `0.0` for training.

## Configuration
To create a neural network model, you must define the architecture of your neural network model in a protocol buffer definition file.

### Common Fields
* `batch_size`: the number of training inputs used per parameter update.
* `step_size`: step size (learning rate) for stochastic gradient descent.
* <a name=configuration-input_shape>`input_shape`</a>: the shape of input data.
* `random_seed`[default=initial seed]: the seed for generating random initial parameters.

### Layers

##### Fully Connected Layer
* Layer type: `FullyConnected`
* Parameters (`FullyConnectedLayerConfiguration fully_connected_param`)
	* `init_weight`: the standard deviation that is used to initialize the weights in this layer from a Gaussian distribution with mean 0.
	* `init_bias`: constant value with which the biases of this layer are initialized.
	* `num_output`: the number of outputs for this layer.

##### Pooling Layer
* Layer type: `Pooling`
* Parameters (`PoolingLayerConfiguration pooling_param`)
    * `pooling_type`[default="MAX"]: the type of pooling for this layer. Available types are MAX and AVERAGE.
    * `padding_height`[default=0]: the space on the border of the input volume.
    * `padding_width`[default=0]: the space on the border of the input volume.
    * `stride_height`[default=1]: the interval at which pooling layers apply filters to inputs.
    * `stride_width`[default=1]: the interval at which pooling layers apply filters to inputs.
    * `kernel_height`: the height of kernel for this layer.
    * `kernel_width`: the width of kernel for this layer.

##### Convolutional Layer
* Layer type: `Convolutional`
* Parameters (`ConvolutionalLayerConfiguration convolutional_param`)
    * `kernel_height`: the height of kernel for this layer.
    * `kernel_width`: the width of kernel for this layer.
    * `padding_height`[default = 0]: the space on the border of the input volume.
    * `padding_width`[default = 0]: the space on the border of the input volume.
    * `stride_height`[default = 1]: the interval at which convolutional layers apply filters to inputs.
    * `stride_width`[default = 1]: the interval at which convolutional layers apply filters to inputs.
    * `init_weight`: the standard deviation that is used to initialize the weights in this layer from a Gaussian distribution with mean 0.
    * `init_bias`: constant value with which the biases of this layer are initialized.
    * `num_output`: the number of outputs for this layer.

##### Activation Layer
* Layer type: `Activation`
* Parameters (`ActivationLayerConfiguration activation_param`)
	* `activation_function`: the activation function to produce output values for this layer.

##### Activation with Loss Layer
* Layer type: `ActivationWithLoss`
* Parameters (`ActivationWithLossLayerConfiguration activation_with_loss_param`)
	* `activation_function`: the activation function to produce output values for this layer.
	* `loss_function`: the loss function that is used to compute loss and calculate the loss gradient for backpropagation.

##### Activation Functions
The following activation functions are supported.
* Sigmoid: `sigmoid`
* ReLU: `relu`
* TanhH: `tanh`
* Power: `pow` (squared value)
* Absolute: `abs`
* Softmax: `softmax`

##### Loss Functions
The following loss functions are supported.
* CrossEntropy: `crossEntropy`

## How to run
A script for training a neural network model is included with the source code, in `run_dnn.sh`. `sample_dnn_data` is a sample subset of the [MNIST](http://yann.lecun.com/exdb/mnist) dataset, composed of 1,000 training images and 100 test images. `sample_dnn_conf` is an example of a protocol buffer definition file; it defines a neural network model that uses two fully connected layers.

You can run a network of the given example on REEF local runtime environment by

```bash
cd $DOLPHIN_ASYNC_HOME/bin
./run_dnn.sh -local true -max_iter 100 -conf sample_dnn_conf -input sample_dnn_data -split 2 -max_num_eval_local 3 -num_worker_threads 1 -timeout 800000 -dynamic false
```

#### Command line parameters

* Required
	* `input`: path of the input data file to use.
	* `conf`: path of the protocol buffer definition file to use.
* Optional
	* `local`[default=false]: a boolean value that indicates whether to use REEF local runtime environment or not. If `false`, the neural network will run on YARN environment.
	* `max_iter`[default=20]: the maximum number of allowed iterations before the neural network training stops.
	* <a name="parameter-delim">`delim`</a>\[default=,\]: the delimiter that is used for separating elements of input data.
	* `timeout`[default=100000]: allowed time until neural network training ends. (unit: milliseconds)

## Example
### A example of protocol buffer definition file for MNIST
The following is the example of a protocol buffer definition file for the MNIST dataset. It can be found at `$DOLPHIN_ASYNC_HOME/bin/sample_dnn_conf`.

```
batch_size: 10
step_size: 1e-3
input_shape {
  dim: 28
  dim: 28
}
parameter_provider {
  type: "local"
}
layer {
  type: "FullyConnected"
  fully_connected_param {
    init_weight: 1e-4
    init_bias: 2e-4
    num_output: 50
  }
}
layer {
  type: "Activation"
  activation_param {
    activation_function: "relu"
  }
}
layer {
  type: "FullyConnected"
  fully_connected_param {
    init_weight: 1e-2
    init_bias: 2e-2
    num_output: 10
  }
}
layer {
  type: "ActivationWithLoss"
  activation_with_loss_param {
    activation_function: "softmax"
    loss_function: "crossEntropy"
  }
}

```
This model comprises two fully connected layers with 50 and 10 features, respectively, and a local parameter provider. `input_shape` specifies the shape of input data. For the MNIST dataset, each data object is a 28 * 28 image and thus `input_shape` is configured as the following.

```
input_shape {
  dim: 28
  dim: 28
}
```
Parameters are updated when every 10 inputs are processed since `batch_size` is specified as 10, and 1e-3 is used as the learning rate for the stochastic gradient descent algorithm.
