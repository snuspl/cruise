# Dolphin: A machine learning framework in Cay with runtime optimization
  This module involves Asynchronous version of Dolphin based on Parameter Server.

### Development of a Unified High-Performance Stack for Diverse Big Data Analytics


## Dashboard

- Dolphin async provides a dashboard service which visualizes the procedure of dolphin applications.
- Dashboard service can be activated by adding `-dashboard {port number for the dashboard}`.
- A dashboard server(flask) will be established on the client machine and other machines can access
  the visualized data by connecting to `http://{host address of the client machine}:{port number}`
- The port number should be an integer between 0 and 66636 which has not been already used. If the
  application fails to bind the port, it will disable the server automatically.
  
##### Requirements
  - Flask(python): `sudo pip install Flask`.
  - The machine must be connected to the internet for downloading resources(e.g., javascript for visualization)
    from CDN and for other machines(e.g., driver) to access.
