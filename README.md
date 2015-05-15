# Spark Hands on

## Summary

1. Prerequisite
2. Exercices

## Prerequisite

### General

* JDK 7 or higher
* SBT 13 or higher
* Your favorite scala IDE (For instance IntelliJ with Scala/SBT plugin)

### Project

* Clone this repository
```
git clone https://github.com/vincentdoba/spark-hands-on.git
```
* Go at root of the cloned repository
* Run word count on this README file 
```
sbt "run-main psug.hands.on.prerequisite.WordCount README.md"  
```

## Exercises

### Exercise 1

#### Description

Determine, given the list of numbers from 1 to 75, the list of numbers from 25 to 100 and the list of prime numbers under 100, the sum of squares of numbers under 100 that are not primes.

#### Notions

* Create a SparkContext
* Create RDD from List
* Discover first action
* Discover transformations

#### Command

```bash
sbt "run-main psug.hands.on.exercise01.SumOfSquaresOfNonPrimeNumbers"
```

### Exercise 2

#### Description

Display the departments whose name contains one of the four major rivers in France, by river's name, given a file containing the list of department

#### Notions

* Load a simple text file
* Create a Key/Value RDD
* Transformations on Key/Value 
* Action : collect


#### Command

```bash
sbt "run-main psug.hands.on.exercise02.DepartmentsCounter"
```

### Exercise 3

#### Description

How many inhabitants has France ?

#### Notions

* Create Spark SQL context
* Load a JSON file
* First actions and transformations on DataFrames

#### Command

```bash
sbt "run-main psug.hands.on.exercise03.TotalPopulation"
```

### Exercise 4

#### Description

What are the ten densest departments in France ?

#### Notions

* DataFrames actions and transformations
* Join

#### Command

```bash
sbt "run-main psug.hands.on.exercise04.DensestDepartments"
```

### Exercise 5

#### Description

Save the demographic characteristics of cities of France in a JSON file ? Preparation of features for Machine Learning Algorithms

#### Notions

* Save to a JSON file
* Some transformations on DataFrames

#### Command

```bash
sbt "run-main psug.hands.on.exercise05.RetrieveFeatures"
```

### Exercise 6

#### Description

Normalize data for a Machine Learning algorithm

#### Notions

* Aggregation transformation

#### Command

```bash
sbt "run-main psug.hands.on.exercise06.Normalization"
```

### Exercise 7

#### Description

Split a data set into two data sets randomly

#### Notions

* Sampling
* Temporary Tables and SQL requests on them

#### Command

```bash
sbt "run-main psug.hands.on.exercise07.DataSetSplitter"
```

### Exercise 8

#### Description

Guess which cities in France have more than 5000 inhabitants given some features such as Density, percentage of executives
in total population, percentage of workers, percentage of employees and percentage of farmers

#### Notions

* Spark Machine Learning Library (MLLib)

#### Command

```bash
sbt "run-main psug.hands.on.exercise08.MachineLearning"
```

### Exercise 9

#### Description

Run the script created in the first exercise (psug.hands.on.exercise01.SumOfSquaresOfNonPrimeNumbers) on a 
Spark cluster

#### Notions

* Spark cluster deployment
* Submit a task to a spark cluster
