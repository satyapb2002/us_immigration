# us_immigration
Udacity Nanodegree Capston

#### Project Summary
In this project I am going to utilize I94 immmigration dataset and US demographic data to analyze the non immigrants travel patterns. Then I am going to create ETL pipeline using Spark to build a data warehouse using a star table schema for further downstream analysis. I will save results in a Parquet file which could be loaded to S3 or a Redshift cluster if needed. 

#### Scope 
In this project I created a data model to analyze the data and discover any relationship patterns between non-immigrant tourists and other metadata available such as demographic information with different preferences in their travel. 

I am going to use Spark to load and clean the data, parquet files to store outputs, and create a data model using star schema for data warehouse tables.

#### Describe and Gather Data 

### I94 Immigration Data: 
This data comes from the US National Tourism and Trade Office. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. 
Some of the important columns are 
- 1) i94Bir 	 Age of non-immigrant in years
- 2) i94visa 	 Visa codes collapsed into three categories
- 3) count 	 Used for summary statistics
- 4) i94mode 	 Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
- 5) i94addr 	 USA State of arrival
- 6) visatype 	 Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

### U.S. City Demographic Data: 
This data comes from OpenSoft. More details about it can be found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
Some important columns are 
- 1) City
- 2) Median Age (overall median age within the city population)
- 3) Total Population
- 4) Male Population
- 5) Female Population
- 6) Count (number of people under specific race category anotated by Race column)

Define the Data Model
#### Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

| Table | Type | Description | Columns | 
| --- | --- | --- | --- | 
| i94-immigration-data | Fact Table | Stores all the count information about imigration data | i94res, i94port, arrdate, i94mode, depdate, i94bir, i94visa, count, gender, admnum, port_city, port_state, arrival_date, MedianAge, Males, Females, TotalPop, Foreign-born, AvgHouseholdSize, NativePopulation, AsianPopulation, BlackPopulation, LatinoPopulation, WhitePoplation |
| resident_city |   Dimension Table | Contains info about resident city of the immigrant | id, country |
| ports | Dimension Table  | Port of entry information | id, city, port  |
| timestamp |  Dimension Table  | Arrival information |  arrival_sasdate, arrival_iso_date, arrival_month, arrival_dayofweek, arrival_year,arrival_day | 
| visa |  Dimension Table  | id | id, type | 
| travel_mode |  Dimension Table  | different travel modes | id, transport | 


#### Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model
- Load I94 immigation data set and dedupe and drop nulls values. 
- Select necessary columns for final analysis and clean null values 
- Build visa type and tranporation mode data frame in spark
- Read port of entry data set and residence country statis data from SAS file
- Join port of entry data frame and drop the duplicate id columns
- Read demographics data set and clean up duplicate and null values
- Aggregate results for different races and store them
- Join with port of entry data and eventually with immigration data frame. This will be the fact table
- Create timestamp dimension table from immigration data frame


#### Data dictionary 

##### timestamp
- PK: arrival_sasdate ( arrival date)
- Transformed columns such as arrival_month, year, day, season from the above column

##### ports
- PK: id(port_id)
- city: Destination city

##### visa
- PK: id (visa_id)
- type: visa type 

##### resident_city
- PK: id (mode_id)
- country

##### travel_mode
- PK: id (mode_id)
- transport: transport type 

##### i94-immigration-data
- PK: admnum (Admission Number)

- arrdate (arrival date) FK : timestamp.arrival_sasdate
- depdate (Departure Date)
- i94port (port id) FK : ports.id
- i94res (resident_city) FK : resident_city.id
- i94mode (travel mode) FK : travel_mode.id
- i94visa (visa id) FK : visa.id
- arrdate (arrival date) FK : timestamp.arrival_sasdate
- age (age)
- gender 

--- City level aggregations 
- MedianAge (Median age of the people in the city)
- Males (Total male poppulation in the city)
- Females (Total female population in the city) 
- TotalPop
- Foreign-born
- AvgHouseholdSize 

--- Race level information about the city. Population of different races based on demographic information
- NativePopulation
- AsianPopulation 
- BlackPopulation
- LatinoPopulation
- WhitePoplation 

#### Project Write Up
##### Clearly state the rationale for the choice of tools and technologies for the project.
I used Pandas and Seaborn for analysis of smaller files. Whereas for larger files Spark was used. Apart from providing fast and in-memory processing of large datasets Spark also provided support to read SAS files as the immigration data was in this format. Eventually I stored the data in parquet file and sent it to S3 where it could be loaded into Redshift for further analysis.

##### Propose how often the data should be updated and why ? 
There are 2 types of data we dealt with. The demographics and Airport data is more static and won't change much. So we could load this once a month or so. They could be also updated manually if any new data is received. Where are the temperature and immigration data is more dynamic and can be updated daily/weekly or monthly based on downstream requirements. This would also depend on the frequency at which the source datasets are updated. 

##### Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 --  We could set up a EMR cluster in AWS and increase the number of nodes to handle the increased data. We could also used cluster manager such as Yarn to manage the resources for us.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 -- Airflow could be used to schedule different steps in the data pipeline. We could setup retries or integrate notification services with pagerduty if any of the steps fails. 
 * The database needed to be accessed by 100+ people.
 -- The parquet files could be loaded into Amazon Redshift cluster in order to achive high performance. Redshift cluster could be optimized to handle massive parallel requests.
