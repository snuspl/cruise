/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var requestTimer;

// When users choose which object to see, change the IDs and y-axis-items to those belong to the objects.
function getIdAndY() {
  clearInterval(requestTimer);
  var objects = $('#objects').val();
  $.post('/selectors', {position:objects}, function(json){
    var data = JSON.parse(json);
    if (data['id'].length > 0) {
      $('#id').html('<option disabled selected hidden>ID</option>');
      var position = (objects === 'Custom')? 'Worker' : objects;
      for (id in data['id']) {
        $('#id').append('<option>' + position + '-' + data['id'][id] + '</option>');
      }
    }
    if (data['y'].length > 0) {
      $('#y-axis').html('<option disabled selected hidden>Y-axis</option>');
      for (y in data['y']) {
        if (data['y'][y] != 'time' && data['y'][y] != 'id') {
          $('#y-axis').append('<option>' + data['y'][y] + '</option>');
         }
      }
    }
  });
}

// reload data from the server every 1 sec to provide real-time visualization.
function reloadData() {
  clearInterval(requestTimer);
  requestTimer = window.setInterval(function(){
    var objects = $('#objects').val();
    var xAxis = $('#x-axis').val();
    var yAxis = $('#y-axis').val();
    var id = $('#id').val();
    // get data for plotting by sending post request to the server.
    $.post('/plot', {position:objects, x:xAxis, y:yAxis, id:id}, function(json) {
      var data = JSON.parse(json);
      var keys = Object.keys(data);
      if (keys.length == 0){
        return;
      }
      keys.sort(function(a, b){return a-b;});
      var trace = {
        x: keys,
        y: keys.map(function(key){return data[key];}),
        type: 'scatter'
      };
      var layout = {
        title: '',
        xaxis: {
          title: 'Time (s)',
          titlefont: {
            size: 18,
            color: '#7f7f7f'
          }
        },
        yaxis: {
          title: yAxis,
          titlefont: {
            size: 18,
            color: '#7f7f7f'
          }
        }
      };
      Plotly.newPlot($('#main-display')[0], [trace], layout);
    }).fail(function() {
      // if the server is stopped, stop sending requests to the server.
      console.log('Stop requesting.');
      clearInterval(requestTimer);
    });
  }, 1000);
}

// when users are changing the metrics to monitor, stop sending requests to the server.
$('.dd-query').change(function(){
  clearInterval(requestTimer);
});
