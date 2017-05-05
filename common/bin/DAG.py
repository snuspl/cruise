# Copyright (C) 2017 Seoul National University
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

# EXAMPLE_USAGE
# python DAG.py {$filepath}
from graphviz import Digraph
from collections import defaultdict
import argparse
import json

parser = argparse.ArgumentParser(description="parse file path.")
parser.add_argument('filepath')
filepath = parser.parse_args().filepath

def showDAG(filename, data) :
	dot = Digraph(comment="The DAG")
	node_dict = defaultdict(str)
	for root in data["rootVertices"]:
		if root not in node_dict:
			node_dict[root] = root
			dot.node(root, root)

	for key, value in data["adjacent"].iteritems():
		if key not in node_dict:
			node_dict[key] = key
			dot.node(key, key)

		for vertex in value:
			if vertex not in node_dict:
				node_dict[vertex] = vertex
				dot.node(vertex, vertex)

			dot.edge(key, vertex)

	dot.render(filename, directory="./output-dag", view=True, cleanup=True)

searchfile = open(filepath, "r")
for line in searchfile:
	# log format : PLANDAG-addOneAssociator{"rootVertices" :[], "adjacents": []}
	if "PlanDAG" in line:
		start_index = line.index("PlanDAG")
		end_index = line.index("{")
		print line[start_index : end_index]
		print line[end_index:]
		showDAG(line[start_index : end_index], json.loads(line[end_index:]))

