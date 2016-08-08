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
from plotly.tools import get_embed

print("Flask script: Building server...")

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
        # TODO: Save the contents
        g.sqlite_db.close()

#
# Urls
#
# The main URL which shows metrics visualization to users. 
@app.route('/', methods=['GET', 'POST'])
def main():
    db = get_db()
    if request.method == 'POST':
        try:
            # update database with new metrics
            id = request.form['id'];
            metrics = json.loads(request.form['metrics'])
            # app-specific metrics
            ml_metrics = metrics.pop('metrics')
            if ml_metrics is not None:
                db.execute('create table if not exists metrics ('
                        + ' varchar(255) not null,'.join(ml_metrics['data'].keys())
                        + ' varchar(255) not null)')
                db.execute('insert into metrics values ('
                        + ', '.join(str(i) for i in ml_metrics['data'].values())
                        + ')')
            # other metrics
            query = list()
            if id.startswith('Worker'):
                query.append('insert into workers values (')
            else:
                query.append('insert into servers values (')
            id = re.sub(r'\D', '', id)
            query.append(id + ', ' + ', '.join(str(i) for i in metrics.values()))
            query.append(')')
            db.execute(''.join(query))
            db.commit()
            return 'accept'
        except:
            return 'error'
    else:
        # get metrics from database
        cur = db.execute('select * from workers')
        worker = cur.fetchall()
        cur = db.execute('select * from servers')
        server = cur.fetchall()
        cur = db.execute('select name from sqlite_master where type=\'table\' and name=\'metrics\';')
        metrics_list = cur.fetchall()
        if not metrics_list:
            return render_template('main.html', workers=worker, servers=server)
        cur = db.execute('select * from metrics')
        metric = cur.fetchall()
        return render_template('main.html', workers=worker, servers=server, metrics=metric)

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
