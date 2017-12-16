# Cruise PS: A Parameter Server implementation in Cruise
  This sub-module of Cruise enables the runtime optimization in the parameter server architecture

## Dashboard

- Cruise PS provides a dashboard service which visualizes the procedure of cruise applications.
- Dashboard service can be activated by adding `-dashboard {port number for the dashboard}`.
- A dashboard server(flask) will be established on the client machine and other machines can access
  the visualized data by connecting to `http://{host address of the client machine}:{port number}`
- The port number should be an integer between 0 and 66636 which has not been already used. If the
  application fails to bind the port, it will disable the server automatically.
  
##### Requirements
  - Flask(python): `sudo pip install Flask`.
