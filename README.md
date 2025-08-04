# GSE electrification analysis
This repo contains the input data and code for the GSE electrification analysis. 

##### Step 1 is to scale up the flight data by using the BTS Airline On-Time dataset and the T-100 Segment dataset to construct a complete, time-resolved flight activity profile at each airport. The flight data obtained from the BTS Airline On-Time dataset is too large and thus not included here. The flight data is available at:

##### Step 2 is to clean up the flight data, removing obvious errors in the flight data.

##### Step 3 is to generate GSE tasks based on the flight data at each airport.

##### Step 4 is to generate GSE events (including both service and charging events) under the 6 defined charging scenarios (S1, S2, S3, S4, S5, S6).

##### Step 5 is to postprocess the charging events for the overnight charging scenarios (S5, S6).

##### Step 6 is to generate GSE load profiles for each airport.

##### Step 7 is to calculate the number of chargers needed for each airport.

##### Step 8 is to calculate the vehicle requirement per GSE type for each airport.

##### Step 9 is to generate the plots in the paper.
