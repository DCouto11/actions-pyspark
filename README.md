# actions-pyspark
Example of Data Engineering Code
- Little application using Pyspark, GitHub Actions and libraries for testing

Index
- Approach and design
- Assumptions or decisions done
- Steps to run the code
- Points of consideration

## Approach and design
When it comes to think about the implementation, it comes with a basic structure and performance
-  main.py -> Python code to run with the calling to different functions.
  - Here, it is taken into consideration the implementation of schemas to ensure data validation.
  - Precisely, it is included additional parameters when it comes to clean the data with several examples (duplicated columns, trimming...)
-  library.py -> Python file with the different functions to be implemented (and also tested).
  - A function for cleaning data.
  - A function to perform all the transformations.
    - The initial approach was to persist the joined dataframe to perform the different operations.
    - That approach should be discarded when data scales and may lower the performance.
-  tests -> Despite it is true that the first thing to do are the test rather than the functions themselves, there was some feedback between them.
  - For instance, differences between file and test input when it comes to data format (LongType vs IntegerType for store_id)
  - In addition, the same implementation of schema should be done at first (as it must be the same behavior than real time execution.

## Assumptions done
In reference to assumptions, there were few in terms of coding and designing the pipeline
- Schemas delivered. This means it is assumed that data should fit in a specific pattern.
- No data validation to specific columns (in some projects, it may be valid to filter rows from a dataframe if a value is wrong. E.g: Discard all rows if column value is a negative value.)
- Cleaning additional files created with writing (to not deal with original csv files due colesce(1).write the csv file).
- Data oriented to logical implementation rather than volume (low amount of rows in original csv files). This is understood as a Proof of Concept rather than a project kick-off.
- Testing implemented to functions. Calling the main process may be done in different ways, causing differences. Library.py file remains by the functions defined.
- Pipeline configured to be executed by push. In this scenario, it is configured to be executed using crontab (UTC 14:30 on a daily basis).

## Running the code
Python-package.yml contains all the information done to execute the pipeline and running the code.
- Take into consideration the use of several actions in jobs/build/steps.
- Also, it is important to say that requirements.txt has the libraries required and its version to be used.
- Then, in a local environment (virtual environment, for instance) both test and run sections can be performed.
- To provide a closure, this is my first time using GitHub actions and there may be things to do better. So any comment or suggestion will be thankful :P

## Points of consideration
In this section, they are explained some blocking points I had during the coding. The most critical one was related with the UDF function to implement.
Turns out that, when it is define in two sentence (definition + transformation into UDF) it may provoke a failure as the run does not recognize the module.
It is fixed using an annotation defining udf and its type of data.
