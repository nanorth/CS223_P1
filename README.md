# CS223_P1

UCI CS223 Spring 2023 Project 1

- Install PostgreSQL for Windows.
    - Port should be set as 5432.
    - Create a database called "testdb" with all privileges granted to user "postgres"

- Import the project as a Maven project in IntelliJ IDEA.
- Install Java JDK with version >= 8.


- To observe more obvious concurrency, the simulation time has been compressed from 20 days to 4 minutes.
- Under default parameters, the experiment will only simulate 12,000 milliseconds of input data (corresponding to 1/20 of the total data).
- Comment out line 23 in SimulationTest `Settings.switch_to_high_concurrency()`, to observe low concurrency data.
