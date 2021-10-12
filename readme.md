# Assignment 4: Spark assignment
This is the Spark Assignment for of Big Data Processing, CSE2520.

# IMPORTANT
Make sure that in | File | Project Structure | Modules the language level is set to 8, as Spark/Scala is not compatible
with Java versions higher than 8.

## Setup of the assignment
The assignment consists of 2 parts, worth 90 points and 5 bonus points:

1. [Spark and RDD's](<src/main/scala/RDDAssignment/readme.md>)
   In this part you will focus on the basics of Spark RDDs. Last question is bonus.

2. [Spark and DataFrames](<src/main/scala/DataFrameAssignment/readme.md>)
   In this part you will focus on the basics of Sparks SQL, and Spark Catalyst.


### Assignment Descriptions
Please note that the example results in the assignments do not come from the dataset we test you on.
They serve as an illustration to what tuples we expect you to return.


## About Spark.
In the lectures you learned about Spark and its core components. By now you should know that Spark
is intended to process large data volumes in a distributed fashion. Even though you will be solving this 
assignment on a single personal computer; as you will not experience the full capabilities of Spark. Nevertheless,
You can still learn to process big data as in the real world.

## Common Errors

1) ``scalac: Error: Error compiling the sbt component 'compiler-interface-2.11.8-59.0'``

- Solution:
  - Change Jdk to 1.8
  - Run Maven compile


2) ``Not enough spark.driver.memory``

- Solution:
  - Import project using maven
  - Run maven compile
  - Increase Idea heap memory

## Logging
The default logging is set to Info. However, this can be adjusted to *Warn* by 
uncommenting lines 24 and 25 of tests.
```
Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)
```

## Grading
### Grade calculation
This assignment contains a bonus question, `bonus_assignment`, giving an extra 5 points.
All questions together add up to 90 points (excluding the bonus assignment).
Your grade for the assignment is `min(10, points/9)`.



### Handing in your solutions
You can hand in your solutions on [CPM](https://cpm.ewi.tudelft.nl).
Create a ZIP archive of the `src` folder and upload this to CPM, the structure of the zip should be as follows:

* src
    * main
        * scala
            * windows.utils
            * RDDAssignment
                * RDDAssignment.scala
            * DataFrameAssignment
                * DataFrameAssignment.scala

### Automatic grading
After handing in your solutions, you can see the results of the automatic grading.
This only shows you an overall grade and which question(s) is/are incorrect,
but will not give details of the failures.\
You are encouraged to write more unit tests yourself to test your solutions.
You can find some help on writing tests in [test/scala/StudentTest.scala](<src/test/scala/StudentTest.scala>).
IntellIJ's [evaluate expression](https://www.jetbrains.com/help/idea/evaluating-expressions.html) might prove useful
during debugging, to inspect during runtime.

**Warning**: the automatic grader may fail if you do any of the following actions:
- change the names of template files
- change function signatures
- use external dependencies that were not in the original `pom.xml` file
- hand in your solutions in another format than the one specified earlier.

If you are in doubt why the grader fails, ask the TAs for more details during the lab sessions.



### Note on CPM
Note that CPM considers a grade of 0 as script disapproved.


### Programming style
The autograder only tests your solutions for correctness. No manual grading will be done, but
very slow solution, or those with side effects might not pass on the auto graders. Some assignments
state that certain operations may not be used, this will be taken into account by the graders
as Spark allows RDD inspection on execution level by inspection of the `debugString`.
