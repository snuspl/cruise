# Copyright (C) 2016 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tensorflow as tf

# Create some variables.
with tf.device("/cpu:0"):
    b = tf.Variable(tf.zeros([100]), name="b")
with tf.device("/gpu:0"):
    W = tf.Variable(tf.random_uniform([784,100], -1, 1), name="W")
    x = tf.placeholder(tf.float32, shape=(100, 1), name="x")
    relu = tf.nn.relu(tf.matmul(W, x) + b, name="relu")

# Add an op to initialize the variables.
init_op = tf.global_variables_initializer()

# Add ops to save and restore all the variables.
saver = tf.train.Saver()

# Later, launch the model, initialize the variables, do some work, save the
# variables to disk.
with tf.Session() as sess:
  sess.run(init_op)
  
  # Use a saver_def to get the "magic" strings to restore
  saver_def = saver.as_saver_def()
  print saver_def.filename_tensor_name
  print saver_def.restore_op_name

  # Save the variables to disk.
  saver.save(sess, 'trained_model.sd')
  tf.train.write_graph(sess.graph_def, '.', 'trained_model.proto', as_text=False)
  tf.train.write_graph(sess.graph_def, '.', 'trained_model.txt', as_text=True)

