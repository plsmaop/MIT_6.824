# MIT_6.824
*My implementation of programming labs of [MIT_6.824 Distributed Systems Spring 2020](http://nil.csail.mit.edu/6.824/2020/schedule.html)*

### [Lab 1: MapReduce](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html)
* Build a simple local MapReduce cluster
* All tests passed
### [Lab 2: Raft](http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html)
* Implement a simple [Raft consensus protocol](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
* All tests passed
### [Lab 3: Fault-tolerant Key/Value Service](http://nil.csail.mit.edu/6.824/2020/labs/lab-kvraft.html)
* Build a fault-tolerant kv store based on the Raft protocol you've implemented with snapshot support
* All tests passed
### [Lab 4: Sharded Key/Value Service](http://nil.csail.mit.edu/6.824/2020/labs/lab-shard.html)
* Build sharded fault-tolerant kv store based on the Raft protocol you've implemented with snapshot support
* I have passed all tests excluding `TestChallenge1Delete`
* I will work on passing all tests later

### Development
* You should read the webpage of each lab for detailed information
* Some labs are broken into small parts and some labs and parts of a lab have dependencies on other labs and parts.
* You can checkout different branches to start doing these labs
* However __PLEASE NOTE THAT THE CODE OF EACH BRANCH CAN ONLY PASS THE TEST OF IT'S BRANCH NAME AND THE PREVIOUS TESTS__. For example, code of branch `lab3b` can only pass `lab3b`, `lab3a` and all `lab2` tests. Code of branch `lab3b` may contain bugs for `lab4` 
