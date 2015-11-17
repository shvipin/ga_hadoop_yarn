# Generic Genetic Algorithm Library for YARN

A library which can be used by clients to impelment Genetic algorithm for running them on hadoop yarn clusters.

##Steps to bring up cluster
1. Install Vagrant.
2. Bring up virtual machines using `$ vagrant up`. You will have 3 machines (NodeA, NodeB, and NodeB) running.
3. Vagrant provisioning will make sure each file has following:
  - Necessary softwares like java, maven, etc installed.
  - It will create user account "huser" and group "hadoop" in all machines.
  - It will copy hadoop binaries to home folder and set up environment variables.
  - It will overwrite requite configuration file to hadoop binary.
  - It will create ssh key.
4. ssh to huser on NodeA `$ ssh huser@127.0.0.1 -p 2222`
5. Copy ssh pub key in NodeA to 
  - NodeA itself using self host and ip address 10.10.10.5
  - NodeB ip address 10.10.10.6
  - NodeC ip address 10.10.10.7
6. Make sure you are able to login NodeA, NodeB, and NodeC from NodeA without password.
7. Format namdenode `$ hdfs namenode -format`. Run this command only once.
8. Start cluster by issuing two commands
  - `$ start-dfs.sh`
  - `$ start-yarn.sh`
9. Run `$ jps` to check which all daemons are running. Check same on all other machines.
10. Stop cluster by issuing two commands
  - `$ stop-dfs.sh`
  - `$ stop-yarn.sh`

##YARN sample app

