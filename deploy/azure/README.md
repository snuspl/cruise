# How to deploy to Azure

## Salt-Cloud setup (on your Macbook)
1. Install dependencies listed in https://docs.saltstack.com/en/latest/topics/cloud/azure.html
	- For the Azure Python SDK: `pip install azure==0.9`
2. Copy the files from here to /etc/salt/ (creating the directory with `sudo mkdir /etc/salt` if it doesn't exist) and do `cd /etc/salt/` (For example, you should see /etc/salt/cloud.providers)
3. Set up .cer and .pem files as in https://docs.saltstack.com/en/latest/topics/cloud/azure.html, and set `certificate_path` in `cloud.providers`
4. In the file `cloud.profiles`, set `ssh_password` (make it difficult!)


## Instance Creation (on your Macbook)

4. At `/etc/salt/`, run `sudo salt-cloud -m eml` to create a master vm and a minion vm as specified in the file `eml`
  - BAD NEWS: Due to the global lock in an Azure Cloud Service, we need to create instances sequentially (10 minutes per instance), (TODO: need to check if saltstack's -P option works)
5. Open the Azure console to check that all instances are up and running
6. For the master node, create new endpoints for ports 8088(YARN) and 50070(HDFS)
7. Do `ssh azureuser@eml.cloudapp.net`, and run `sudo salt '*' cmd.run 'echo hi'` to see 2 responses (including the master)
8. Yay! Now we have 2 running Ubuntu Azure vms with SaltStack installed!
  - If you want more instances(e.g., 150), just specify new `eml-minion`s in `eml` accordingly (their `port` and `ssh_port` should be different)

## Package/Configuration setup (on eml.cloudapp.net)

Copy and paste `install_reef.sh` (this is required to use the path `salt://install_reef.sh ` in the next step)

```
sudo mkdir /srv/salt
sudo vi /srv/salt/install_reef.sh # Copy and paste install_reef.sh
```


Run `install_reef.sh ` on all running instances (including the master) to install java/maven/hadoop/reef, this takes about 10 minutes

```
sudo salt --output=quiet '*' cmd.script salt://install_reef.sh runas=azureuser output_loglevel=quiet

```

Time to configure for Hadoop! First, set up `/etc/hosts`

```
sudo vi /etc/hosts

# Add the following mappings:
# 10.0.2.4 master-eml
# 10.0.2.5 slave-eml

sudo salt-cp '*' /etc/hosts /etc/hosts # Send to all instances

```

Second, enable no-password-ssh from master to any minion

```
sudo salt-cp '*' /home/azureuser/.ssh/authorized_keys /home/azureuser/.ssh/authorized_keys
```

Finally, edit hadoop configurations, referring to the example configurations in this directory

```
# After adding JAVA_HOME and HADOOP_HOME to hadoop-env.sh...
sudo salt-cp '*' /home/azureuser/hadoop/etc/hadoop/hadoop-env.sh /home/azureuser/hadoop/etc/hadoop/hadoop-env.sh

# Do similar things for core-site.xml, yarn-site.xml, slaves
# core-site.xml: hostname of master for HDFS
# yarn-site.xml: hostname of master for YARN
# slaves: hostnames of slaves
```

You should be able to start/stop Hadoop now and see the UI at  `eml.cloudapp.net:8088(50070)`! If `HelloREEFYarn` runs okay, then you're now set! Whenver you need to make changes to all running instances(e.g., Hadoop configurations), you can use the SaltStack comamnds(e.g., `salt`, `salt-cp`).


## Instance Stop/ReStart/Delete

Go to Azure console and in Cloud Service panel, click stop, which will then stop all instances concurrently in the Cloud Service. 

You can also Restart them all at once and SaltStack daemons will be restarted automatically so that you can still use the SaltStack commands. Also, the instances I think start in alphabetical order so that the instances will orderly match 10.0.2.4, 10.0.2.5, ...

When deleting, please also delete the disks so that we don't get charged for orphaned disks.


## Questions?

If you run into any trouble, please ask me(@johnyangk) and I'll be happy to help!
