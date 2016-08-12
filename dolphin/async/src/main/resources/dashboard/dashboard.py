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
import json
import os
import re
import sqlite3
import sys
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash

print("Flask script: Launching dashboard server...")

#
# Configurations
#
app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'metrics.db'),
    SECRET_KEY='development key',
    USERNAME='admin',
    PASSWORD='default'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

#
# Database
# Create, connect database(sqlite3), save the database when shutting down.
#
def connect_db():
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv

def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db

def init_db():
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'sqlite_db'):
        # TODO #706: Save the contents
        g.sqlite_db.close()

#
# Urls
#
# The main URL which receives metrics visualizes the metrics to users.
@app.route('/', methods=['GET', 'POST'])
def main():
    db = get_db()
    if request.method == 'POST':
        # update database with new metrics
        id = request.form['id'];
        time = request.form['time'];
        metrics = json.loads(request.form['metrics'])
        ml_metrics = metrics.pop('metrics')
        # metrics
        if id.startswith('Worker'):
            position = 'worker'
        else:
            position = 'server'
        id = re.sub(r'\D', '', id)
        db.execute('insert into {0} values ({1}, {2}, {3})'
                   .format(position, time, id, ', '.join(str(i) for i in metrics.values())))
        # app-specific metrics
        if ml_metrics is not None:
            db.execute('create table if not exists metric (time double not null, id int not null, {0} varchar(255) not null);'
                       .format(' varchar(255) not null, '.join(ml_metrics['data'].keys())))
            db.execute('insert into metric values ({0}, {1}, {2})'
                       .format(time, id, ', '.join(str(i) for i in ml_metrics['data'].values())))
        db.commit()
        return 'accept'
    else:
        return render_template('main.html')

@app.route('/plot', methods=['POST'])
def plot():
    position = request.form['position'].lower();
    x = request.form['x'];
    y = request.form['y'];
    id = re.sub(r'\D', '', request.form['id']);
    db = get_db()
    data = dict()
    try:
        cur = db.execute('select time, id, {0} from {1}'.format(y, position))
        for row in cur:
            if row[1] == int(id):
                data[row[0]] = row[2]
    except:
        data = dict()
    return json.dumps(data)

@app.route('/selectors', methods=['POST'])
def selectors():
    position = request.form['position'].lower()
    db = get_db()
    try:
        cur = db.execute('pragma table_info({0})'.format(position))
        y_axis = map(lambda x: x['name'], cur.fetchall())
    except:
        y_axis = ['no data yet']
    try:
        cur = db.execute('select id from {0}'.format(position))
        ids = sorted(set(map(lambda x: x[0], cur.fetchall())))
    except:
        ids = ['no data yet']
    return json.dumps({'id':ids, 'y':y_axis})

#
# Main
#
if __name__ == '__main__':
    with app.app_context():
        init_db()
        try:
            app.run(host='0.0.0.0', port=sys.argv[1])
        except:
            print('Flask script: Invalid port number')
            sys.exit(1)
