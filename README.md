# capstone project

## Domain
You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.) via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
Now the business wants to know the efficiency of the campaigns and channels. Let’s help them out!

### Given datasets
https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS

### Tasks #1.Build Purchases Attribution Projection

- **_Task #1.1._** Implement it by utilizing default Spark SQL capabilities.

- **_Task #1.2._** Implement it by using a custom Aggregator or UDAF.

Implemented in Task1Job.scala as **purchAttrProjGen** using Spark SQL (use **_runT1()_** to run)
and **purchAttrProjAggGen** using custom Aggregator (use **_runT2()_** to run). 



### Task #2. Calculate Marketing Campaigns And Channels Statistics

- _**Task #2.1.Top Campaigns:**_
What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
- **_Task #2.2.Channels engagement performance:_**
What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements) with the App in each campaign?

_Requirements for task #2:_
  - Should be implemented by using plain SQL on top of Spark DataFrame API
  - Will be a plus: an additional alternative implementation of the same tasks by using
  Spark Scala DataFrame / Datasets API only (without plain SQL)

Implemented in Task2Job.scala as **topMarkCampGen**, **topMarkCampAltGen**(alternative version) for task2.1 
and **topSesChanGen**, **topSesChanAltGen** for task2.2. Use **_runTaskNum(_** taskNum: String **_)_** to run, 
where taskNum = ["task21", "task21alt", "task22", "task22alt"]

### Task #3.Organize data warehouse and calculate metrics for time period.
- **_Task #3.1._** Convert input dataset to parquet. Think about partitioning. Compare performance on top CSV input and parquet input. Save output for Task #1 as parquet as well.
- **_Task #3.2._** Calculate metrics from Task #2 for different time periods:
  - For September 2020
  - For 2020-11-11
  
  Compare performance on top csv input and partitioned parquet input. Print and analyze
  query plans (logical and physical) for both inputs.

_Requirements for Task 3_
  - General input dataset should be partitioned by date
  - Save query plans as *.MD file. It will be discussed on exam.

Task 3.2 implemented as function **runDateAndFormat(dateStart: String, dateEnd: String, format: String, version: String, givFun: Dataset[PurchAttrProj] => Dataset[_])**. 
Use **runDateAndFormatV1(dateStart, dateEnd, format)** to run Task3.2 with topMarkCampGen
and **runDateAndFormatV2(dateStart, dateEnd, format)** to run Task3.2 with topSesChanGen.
