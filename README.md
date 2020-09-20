
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

