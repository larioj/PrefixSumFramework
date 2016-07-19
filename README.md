# Prefix Sum Framework

This is a Mesos framework that implements the [prefix sum parallel algorithm](https://en.wikipedia.org/wiki/Prefix_sum). This algorithm
is historically important because it the first step in a parallel sort implementation.

## Architecture

| File                                     | Explanation                                                |
| ---------------------------------------- | ---------------------------------------------------------- |
| src//main/scala/PrefixSum.scala          | This is the entry point of the scheduler                   |
| src//main/scala/PrefixSumExecutor.scala  | This is the executor                                       |
| src//main/scala/PrefixSumScheduler.scala | This is the scheduler                                      |
| src//main/scala/PrefixSumState.scala     | This is the state tracker                                  |

The framework is divided into three components: scheduler, executor, and state tracker. The scheduler and the
executor are the Mesos components we need to implement, while the state tracker is the domain specific code
that helps to maintain track of what operations have been done, what is currently being done, and what can
be done in the future.

### Scheduler
This is the main component that we are responsible for writing when building a framework. At the simplest level
this component is responsible for handling resource offers, scheduling tasks, and handling the status updates
of the running tasks. There are many methods that you must overide from mesos.Scheduler, but the two most
important ones are resourceOffers and statusUpdate. In the resourceOffers method the scheduler receives a
list of offers. It can then either decline the offers or transform them into a list of tasks to be launched.
In this implementation the scheduler uses the resource offers to perform the tasks that it receives from
the state tracker.

### Executor
This is the second component of a Mesos framework. This component runs on all the agents that will perform
the tasks. This component is optional since for many tasks you could just use the default commandExecutor.
However, in our situation we needed to communicate data between the executor and the scheduler, so a
custom executor was necessary. The executor needs to subclass mesos.Executor, and
implement all the necessary methods. Most of the methods can be left blank for a simple executor. The main
method that must be implemented is the launchTask method, which takes care of launching the tasks given
sent to the executor. It is recommended that you launch a new thread to execute the task, but in this
framework it is more efficient to just perform the operation.

### State Tracker
The state tracker is a domain specific object that keeps track of the progress of the algorithm. It has a
very minimal interface. The customer of this object can get work items, submit a result from a work item,
or submit the failure of a work item. This makes it so that the scheduler itself is not aware of the
 specifics of the algorithm, and can just focus on it job of launching tasks and receiving updates. Because
of this interface the scheduler does have to keep a mapping from framework tasks to algorithm tasks, but this
mapping is fairly simple.

## Installation

* Follow the instructions to install the vagrant environment [here](https://github.com/dcos/dcos-vagrant)
	* Use the 1.7 configuration
	* Deploy with ``` vagrant up m1 a1 p1 boot ```
	* The dcos-vagrant cloned directory will be mounted on all the machines at /vagrant
* Clone this repository
* Create a jar of the project with ``` sbt assembly ```
	* The jar will be created in ``` target/scala-2.11/PrefixSumFramework-assembly-1.0.jar ```
* Move the jar to the vagrant-dcos directory
* ssh into m1 with ``` vagrant ssh m1 ```
* Run the scheduler ``` java -cp /vagrant/<jar-name> PrefixSum ```
* Watch as it does cool things 

## Recommended Reading
* Mesos in Action (Chapter 10)
* Apache Mesons Essentials (Chapter 7)

