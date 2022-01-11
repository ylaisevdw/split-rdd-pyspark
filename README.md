# split-rdd-pyspark
## Goal
We wish to validate whether some RDD (RDD1) is a subset of some other RDD (RDD2). However, these RDDs are very large. Hence, we wish to split both RDDS in the same way into smaller RDDs and place them all in an array. We therefore first partition both RDDs similarly, such that similar values in RDD1 and RDD2 have a similar partition ID. 
The idea is that the elements at each index (one partition of the splitted RDD) of both arrays should match. Hence, we can loop over both arrays and first compare array_rdd_1[0] with array_rdd_2[0], then array_rdd_1[1] with array_rdd_2[1], and so on. If we notice that one of the indices does not match, we can already argue that RDD1 is not a subset of RDD2 without going over the entire (very large) RDDs. 
