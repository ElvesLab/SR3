# Welcome to SR3: customizable recovery for stateful stream applications

## Runtime Environment:

* OS: Linux
* Machine: local servers or Amazon EC2 machines
* Requirements: Java 1.8, Pastry, Strom

### Packages and Requirements
- The system should be implemented upon multiple servers. 
  We sugeest to use at least 5 servers/VMs/EC2 machines to deploy it
- Before deploy it, make sure the servers are Linux OS and with Java 1.8 installed
- The system requires jar packages. We lists the packages here: 
  - FreePastry-2.1.jar
  - commons-io-2.6.jar
  - commons-lang-2.6.jar
  - Storm package needs to use the forked version in our lab respository:  [Storm](https://github.com/fiu-elves/storm)

## Steps of test for functionality

* The system uses eth0 ports to communicate at runtime, you need to identify the part that the system can use, e.g., 10.128.65.xxx. This info should be changed in the code where Pastry builds many nodes.
* Check the bandwidths between servers, you can run the system with/without bandwidth limits
* Use a script to start the system at each server/machine simultaneously (This is to deploy many nodes at different servers)
* You can choose different stream applications from Yahoo streaming benchmarks, which can be used to create interim state.
