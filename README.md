# kafka-algorithm
Create an app to reduce the speed of processing of less data topics compared with the huge topics

## Important
If you are trying to run this application, you should consider few things before starting:
  - This will work only with the single partition topic.
  - Define proper value for delta values.
  - Hardcode the maxTime variable as per your topic data for now, as I need to add the functionality for this in next version.
  - Try with some wide range of data, Since I need to run more test cases on closer ranges.
  
## Changes
  - Removed saving maxTime in kafka topic and introduced the zookeeper since the compacted topics are hard to maintain.
  - Added the consumer lag property into the algorithm.
  
## TO DO
* Avoid using direct APIs, Follow mirror maker code. (This can be done in the end.)
* Approach for multiple partitions.
* Generalize functionalities
* Test feasibility of using consumer lag.
