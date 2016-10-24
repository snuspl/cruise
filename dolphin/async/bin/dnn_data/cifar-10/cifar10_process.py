#!/bin/sh
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

# EXAMPLE USAGE
# ./cifar10_process.py [cifar10-python-data-directory-path] [cifar10-processed-data-directory-path]

import sys

train_file_list = ['data_batch_1', 'data_batch_2', 'data_batch_3', 'data_batch_4', 'data_batch_5']
test_file_list = ['test_batch']
label_file = ['labels']
imageSize = 3072

def unpickle(file):
    import cPickle
    fo = open(file, 'rb')
    dict = cPickle.load(fo)
    fo.close()
    return dict

def save_processed_image(dict, filename, isValidation):
    fo = open(filename, 'w')
    dataSize = len(dict['data'])
    for i in xrange(dataSize):
        data = dict['data'][i]
        for j in xrange(imageSize):
            fo.write("%d" % data[j])
            fo.write(',')
        fo.write("%d," % dict['labels'][i])
        fo.write("%d" % isValidation)
        fo.write('/n')
    fo.close()


cifar_original_path = sys.argv[1]
cifar_processed_path = sys.argv[2]

for filename in train_file_list:
    dict = unpickle(cifar_original_path + "/" + filename);
    save_processed_image(dict, cifar_processed_path + "/" + filename, 0)

for filename in test_file_list:
    dict = unpickle(cifar_original_path + "/" + filename);
    save_processed_image(dict, cifar_processed_path + "/" + filename, 1)

