Flexion
=======

Note: code is still under development.

### How to build Flexion
1. Build REEF: check https://cwiki.apache.org/confluence/display/REEF/Compiling+REEF

2. Build shimoga with shimogapp:
    ```
    git clone https://github.com/cmssnu/shimogapp
    mvn build install
    ```

3. Build Flexion:
    ```
    git clone https://github.com/swsnu/Flexion
    mvn build install
    ```
    
### How to run Flexion
In order to run a application with Flexion, you must at least write a class that implements `UserComputeTask`, a class that implements `UserControllerTask`, and a Launcher class that uses `FlexionLauncher`.

In case you just want to test out Flexion without writing any code, samples are provided for you: `SimpleCmpTask`, `SimpleCtrlTask`, and `SimpleREEF` implement the classes mentioned above, respectively. There is also a run script provided, `bin/run.sh`. Simply execute it without any additional arguments, and Flexion will run `SimpleREEF`.
Currently `SimpleCmpTask` does not perform any special actions. Instead for demonstration, it spends a few milliseconds and then returns. The time spent during this meaningless computation is sent to the Driver. You can check that the Driver has received message like `Task Message CmpTask-1: 47 milliseconds` in its `driver.stderr` log. Although the Driver does not perform further actions, we can use the collected info to later build a optimization plan.

### Why run Flexion?
As mentioned above, the only code you need to write to run your applcation Flexion is, `UserComputeTask`, `UserControllerTask`, and a Launcher. This greatly reduces your work to write long, hard code!
