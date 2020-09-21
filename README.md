
### US Immigration Data Engineering Project

Curious to explore the US immigration data insights? Lets dive in.

#### Outline 

The US immigration data is take from [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). This contains I94 record contains the monthly information of international visitor information. For this project we will be using the data for 2016.

The project follows the follow steps:

1. [Scope the Project and Gather Data](#Scope-the-Project-and-Gather-Data)
2. [Explore and Assess the Data](#Explore-and-Assess-the-Data)
3. [Define the Data Model](#Define-the-Data-Model)
4. [Run ETL to Model the Data](#Run-ETL-to-Model-the-Data)
5. [Discussions](#Discussions)

#### Scope the Project and Gather Data

In this project, the main data comes from the I94 Immigration data which contains the port the immigrant entered into US, using which mode of transport, airlines, flight number, arrival date, departure date, purpose of visit, visa type, and his personal info such as age, gender, nationality, occupation, etc. This data is combined with city demographics to analyze the preference of cities entering into US with the demographics. Furthermore, the airport codes data along with the immigration data provides a great way to analyze the traffic at airports.

I- 94 Immigration data Source:  [Visitor Arrivals Program (I-94 Record)](https://travel.trade.gov/research/reports/i94/historical/2016.html)

The Immigrants data contains the following columns:

- cicid: unique for each row in a month
- i94yr: 4 digit year
- i94mon: numeric month
- i94cit: non-immigrant citizenship country code
- i94res: non-immigrant resident country code
- i94port: non-immigrant port of entry code
- arrdate: arrival date in the US (sas date numeric)
- i94mode: mode of entering into US - Air/Sea/land/Not Reported
- i94addr: address in the US
- depdate: departure date from the US (sas date numeric)
- i94bir: non-immigrant's age
- i94visa: non-immigrant's visa category - Business/Pleasure/Student 
- dtadfile: date added ti i94 files
- visapost: Department of State wher Visa was issued
- occup: occupation that will be performed in the US
- entdepa: arrival flag
- entdepd: departure flag
- entdepu: update flag
- matflag: match flag
- biryear: 4 digit birth year
- dtaddto: date to which admitted to US
- gender: non-immigrant gender
- insnum: INS number
- airline: Airline used to arrive in US
- admnum: Admission Number
- fltno: flight number of airline
- visatype: Class of admission legally admitting the non-immigrant to temporarily stay in US

US City Demograohic Data: [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

This data contains the following columns:

- City
- State
- Median Age
- Male Population
- Female Population
- Total Population
- Number of Veterans
- Foreign-born
- Average Household Size
- State Code
- Race
- Count: Number of people of given race

Airport Code Table: [datahub](https://datahub.io/core/airport-codes#data)

The Airports Codes data contains the list of airports/heliports of all kinds (small, medium and large) in the world. The followqing are the columns in this data:

- ident: unique identity to each row
- type: heliport/small_airport/medium_airport/large_airport
- name: Airport Name
- elevation_ft: elevation in feet from ground
- continent
- iso_country: iso_country code in alpha_2
- iso_region: iso_region code in alpha_2
- municipality: City of Airport
- gps_code
- iata_code
- local_code
- coordinates

End Use Cases:

1. Analyze the immigrants preferences of cities/states entering into US based on demographics.
2. Analyze the immigrants traffic at airports, cities, and states.
3. Analyze the immigrants preferences of airlines.

#### Explore and Assess the Data

Data in its available form is very dirty to work with and requires a cautious cleaning so as to derive insights from it. Firstly, data dictionary for the I94 ports is crucial as it connects to the other data sources with city, state and country. This information enables us to connect with the other data. 

**Cleaning the I94 Immigration Data**

I94 ports giving the information of port of  entry mapping consists of the city/port/county/crossing and US province/state/Country. It is difficult to match it with the other data when such irregularities are present. We need: `locality(city), state/province, country` inorder to match it with the other data. Hence, a care ful cleaning of the I94 ports data is done as follows:

Firstly, th port of entries in US and Non-US is separated.  The following cleanup process is taken in the Non-US regions:

1. Each entry's airport/passing, city, state or province, country is identified.

    Reason: To match with the airport codes data.
    
   For example:
   
	   'TOK'	=	'Torokina #ARPRT, New Guinea'
   
	   is changed to:
   
	   'TOK'	=	'Torokina Airport, Torokina, Bougainville, New Guinea'
   
   'Code'	=	Airport/Crossing, City, State, Country
   
   This matches with the muncipality 'Torokina' in the Airports code data.

The next clean up process for the US region:
1. Separating out the US ports and Non-US ports.
2. Replacing the shortcuts like 'INTL' with International, and 'ARPRT' with Airport.
3. Replacing the combined airports like:

        OAKLAND COUNTY - PONTIAC AIRPORT, PONTIAC, MI AS OAKLAND COUNTY - PONTIAC, MI
4. Handling the typos:

        HAMLIN, ME as HAMIIN, ME
5. Adding the city name, if airport name is found in I94 port definition:
        
        Given: BILOXI REGIONAL, MS
        Changed to: BILOXI REGIONAL AIRPORT, BILOXI, MS

6. Identified crossings and bridges in the I94 ports.
7. Made sure that every I94 port has an associated city which is marked as entering city/Town. This in referred as locality in the coming sections.
8. Each entry's airport/passing, city, state or province is identified.

    Reason: To match with the airport codes data.
    
   For example:
   
	   'MOS'	=	'MOSES POINT INTERMEDIATE, AK'
   
	   is changed to:
   
	   'MOS'='Moses Point Intermediate Airport, MOSES POINT, AK'
   
	   'Code'	=	Airport/Crossing, City, State
   
   This matches with the muncipality 'Moses Point' in the Airports code data and cities demographics data.

This modified data is stored in the files:
`I94_Ports_Non_US.txt` and `I94_Ports_US.txt`. Furthermode, these are cleaned up and saved as cleaned data `I94_ports.csv` in `Cleaned Data/` folder.

Code for the clean up process is found [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project/blob/master/Preprocessing%20%2C%20Cleaning%20and%20Exploring/Cleaning%20the%20US%20I94%20ports.ipynb) 

 The `I94_ports.csv` file contains:
- code
- port
- locality
- province 
- territory

The other cleanup process with I94 immigration data is the non-immigrants nationality data. This is found [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project/blob/master/Preprocessing%20%2C%20Cleaning%20and%20Exploring/Cleaning%20the%20countries%20mapping%20for%20Immigation%20data.ipynb) 

**Cleaning the Airports Code data**

In order to connect I94 data with the Airports data we use `municpality`, `iso_country` and `iso_region` columns. Here, the municipality denotes the city, and iso_region has the combined information of state/province and country. For instance, the `US-KS` iso_region denotes the country United States and the state of Kansas. So, we use the `pycountry` package to decode the iso_region into state and country as follows:

Installation pycountry package:
```
$ pip install pycountry
```

Import pycountry package and use as follows:
```
>> import pycountry as pyc

## Extracting the state from iso_code column
>> print(pyc.subdivisions.get(code='US-DC'))

Subdivision(code='US-DC', country_code='US', name='District of Columbia', parent_code=None, type='District')

## Extracting the country from iso_country column
>> pyc.countries.get(alpha_2='US')
Country(alpha_2='US', alpha_3='USA', name='United States', numeric='840', official_name='United States of America')

```

Another important cleaning of data is the handling of the latin words in the name of airports/municipalities. A best example is `Corazón de Jesús Airport`. This is read wierdly in the UTF-8 encoding. Hence this should be changed to human readable format using the unidecode package as shown below:

Installation of the unidecode package:

 ```
$ pip install unidecode
```

Using the unidecode:

```

>> s = "CorazÃ³n de JesÃºs Airport"
>> s.encode(encoding='latin-1',errors='strict').decode()
'Corazón de Jesús Airport'
>> from unidecode import unidecode
>> unidecode(s.encode(encoding='latin-1',errors='strict').decode())
'Corazon de Jesus Airport'
```

Reason for conversion: the cities available in other data are in human readable format.

This cleaning process can be found [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project).

The final clean data with extra columns so as to connect with the new columns municipalityL and nameL, municipalityE and nameE, state, and country where `L` denotes Latin and `E` denotes English letters in name and municipality. This data is stored as parquet file in the cleaned data folder as `AirportsData`.


**Exploring for the missing and duplicate data in I94 immigration data**

We find the lots of missing data in columns of visapost, occup, entdepu, and insnum. Hence, these columns are not plcaed in the final data warehouse.

Observations:

1. `admnum` is not unique to each row
2. `cicid` is  unique for each row within a month 

This is verified from the number of distinct `cicid`, `admnum` and `total number of rows` for april and may months combined data.

Exploration code can be found [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project/blob/master/Exploring%20the%20US%20Immigration%20data.ipynb).

#### Define the Data Model

Facts Table:

I94 Immigration data:

Columns:
 - cicid
 - i94yr
 - i94mon
 - i94cit
 - i94res
 - i94port => 3 letter i94 port of entry code
 - iso_arrdate
 - iso_depdate
 - iso_duedate
 - i94_visa
 - i94_mode
 - admnum
 - insnum
 - i94addr_US_state  
 - airline
 - fltno
 - visatype
 - i94bir
 - gender

Dimensions Tables:


Port of entries code table:

- code => 3 letter i94 port of entry code
- port => Airport or crossing
- locality => city/town/municipality
- province => state/province
- territory => country

Airports Table:

- code => 3 letter i94 port of entry code
- ident => unique  Column
- type
- elevation_ft
- continent
- gps_code
- iata_code
- local_code
- coordinates
- nameL
- municipalityL

Cities Table:

- code => 3 letter i94 port of entry code
- Median_Age
- Male_Population
- Female_Population
- Total_Population
- Number_of_Veterans
- Foreign-born
- Avg_Household_Size
- Race
- Count
- Race_percent_by_city
- Race_rank_by_city

<div align='center'>
<img src="/Images/Project Data Modeling.png" height="600" width="800">
</div>

**Data Model Discussions**

In the above described data model, the soul of the data lies at I94 port of entry, this is availablein i94port in Immigration data and is connected with the airports and cities demographics data using this code alone. In order to get the port of entry information we have its own dimension table. In this way, the data is easily joined with this column alone to derive insights. From the figure, it is evident that it is a **Star** schema.

**Pipeline Steps**

The code for the following steps is found in `etl.py`.

1. The cleaned data is available in the `Cleaned Data` folder.  The I94_ports_code.csv is one dimension table. And, the steps taken to get such clean data is described [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project/blob/master/Preprocessing%20%2C%20Cleaning%20and%20Exploring/Cleaning%20the%20US%20I94%20ports.ipynb).
2. Next the Airports dimension data is taken from the `AirportsData` folder in the `Cleaned Data` and then merged with the I94 POE codes data on columns of city, state, and country. So, as to provide each matched row with a POE code. And, then saved as a parquet file in S3.
3. The other dimension data for demographics of the cities is also merged with the I94 POE codes data on columns of city, state, and country. So, as to provide each matched row with a POE code. And, then saved as a parquet file in S3. 
4. Finally, the I94 Immigration records data is taken and dates columns are handled to create a valid date as the given date is in sas date format. Then, the citizen and resident country codes are replaced with actual countries. Also, the US states codes also replaced with actual states for the non-immigrants address.  The visa types also replced with Business, Pleasure and Student, along with the mode of entry to the US. After, all these necessary changes the immigration information is saved as parquet file partitioned by year, month, and state. The step-by-step break down of the entire process can be found [here](https://github.com/VarunRaj7/US-Immigration-Data-Engineering-Project/blob/master/Exploring%20the%20US%20Immigration%20data.ipynb). 
5. In the end, a few data quality checks are made to enure proper data is saved.

#### Run ETL to Model the Data

The `Cleaned Data` is crucial to model the data. It is also available as the S3 bucket in public for now. Or one can move it from local to S3 bucket to directly access from the AWS EMR. The `etl.py` does all the ETL steps discussed above. 

```
$ spark-submit --master yarn \
>> --conf spark.shuffle.service.enabled=true \
>> --conf spark.dynamicAllocation.enabled=true \
>> --conf spark.dynamicAllocation.minExecutors=1 \
>> --packages saurfang:spark-sas7bdat:3.0.0-s_2.11
>> etl.py
```

#### Discussions

**Technology**
 
Used Apache Spark for the ETL process throughout, as we can perform the ETL process in a distributed manner and take advantage of the joins in the Spark library. Especially, the **Broadcast Joins** are crucial as it enables to join large dataframe with a small dataframe efficiently. The dataframes are stored as parquet files in the S3 that acts as a Data Lake. 

**Goals**

The end goal is to combine the I94 data with the ports and port of entry cities demogrphics and analyze the patterns. 

**Sample Queries**

**Why this data model?**

This **Star schema** data model is quick to understand and run queries by the business teams to derive insights.
 
**Data Updates**

Whenever a new port of entry code is added in the I94 immigration data it should be updated in the I94 ports dimension as well as the other dimesions for the new city, state, and country. Also, the demographics data should also be updated for every new census. Finally, the I94 immigration data can be updated depending on the frequency of how the data is received. For instance, if we receive monthly data we can run the etl process for the facts data. 

**Scaling?**

What if the I94 Immigration data scales up by 100x?
Increase the size of the cluster appropriately, and perform the ETL process. The ETL process is scalable as it contains mainly the broadcast join only. 

What if the pipelines were run on a daily basis by 7am?
Schedule a Spark Job via Airflow daily by 7am.

What if the database needed to be accessed by 100+ people?
Create an appropriate policies for different user groups to read the objects in the S3 bucket.
