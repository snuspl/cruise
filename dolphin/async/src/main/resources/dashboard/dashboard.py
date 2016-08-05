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
import os
import sys
import json
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, render_template, flash
from plotly.tools import get_embed

print("building server...")

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
        db.execute('insert into entries (workerid, serverid) values (?, ?)',
                [request.form['serverid'], request.form['workerid']])
        db.commit()
        return 'accept'
    else:
        cur = db.execute('select * from entries')
        entries = cur.fetchall()
        print(entries)
        return render_template('main.html', metrics=entries)

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
