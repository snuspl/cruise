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
b = tf.Variable(tf.zeros([100]), name="b")
W = tf.Variable(tf.random_uniform([784,100], -1, 1), name="W")
x = tf.placeholder(name="x", name="x")
relu = tf.nn.relu(tf.matmul(W, x) + b, name="relu")

# Add an op to initialize the variables.
init_op = tf.global_variables_initializer()

# Add ops to save and restore all the variables.
saver = tf.train.Saver()

# Later, launch the model, initialize the variables, do some work, save the
# variables to disk.
with tf.Session() as sess:
  sess.run(init_op)
  # Save the variables to disk.
  save_path = saver.save(sess, "./trained_model.proto")
  print("Model saved in file: %s" % save_path)