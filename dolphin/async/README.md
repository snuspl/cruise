# CAY: Unified Big Data Analytics Stack

### Development of a Unified High-Performance Stack for Diverse Big Data Analytics


## Dashboard

- Dolphin async provides a dashboard service which visualizes the procedure of dolphin applications.
- Dashboard service can be activated by adding `-dashboard {port number for the dashboard}`.
- A dashboard server(flask) will be established on the client machine and other machines can access
  the visualized data by connecting to `http://{host address of the client machine}:{port number}`
  
##### Requirements
  - Flask(python): `sudo pip install Flask`.
  - The machine must be connected to proper internet.
