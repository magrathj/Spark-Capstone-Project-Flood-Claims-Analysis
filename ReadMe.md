# Spark Capstone Project: Flood Claims Analysis (NFIP Claims and Policy Datasets)
*This analysis is part of the Udacity Data Science Nanodegree and is no reflection of my current employer*

* [Medium Blog](https://medium.com/@magrathj/fema-publishes-nfip-claims-and-policy-data-6535f33f8c79)


## Project Overview

The National Flood Insurance Program is a US Government agency which provides insurance to flood prone areas in the US. The Flood market has recently been opened to the private market and the NFIP has recently provide their claims and policy information to the public. 

This project analyses the NFIP claims and policy datasets, using spark and data bricks. The aim of the project is to utilise/analyse spark to handle the large dataset released by the NFIP and to produce loss cost model of the claims in a specific region. 

## Problem Statement

The aim of the project is to use Spark, on Data Bricks cloud platform, to see if we can handle a large volume of claims and policy data from the NFIP and transform it to be able to predict the loss cost for a region. Being able to predict the loss cost will enable insurance companies to set a minimum premium for a  policy given certain characteristics.

## Metrics

The GLMs were evaluated using two metrics, Root Mean Squared Error (RMSE) and R squared, on the test dataset.

## Analysis

The analysis format of the dataset but also investigated the claims disturbtion. From analysising the claims dataset, the effects of Hurricane Sandy and its impact on NY state became very clear.


## Methodology

* In order to perform the analysis, a lot of data preparation had to be performed beforehand. Firstly, I used spark to merge all of the NFIP policy data together and then cut the data by New York state.

* From there, I analysed the data asking some general questions to understand the claims distribution and profitability of the portfolio.

* Then I tranformed the datasets and merge them into a tabular form to be able to apply a loss cost model (frequency and severity model combined). 

* I used a gamma model to measure the severity of the claims and a poission model to measure the frequency of the claims.

* Lastly, I evalated the results using R squared and RSME metrics.

## Results

* The results were quite off, this is mainly due to the fact that we did not remove the extreme event of Sandy from the datasets. This would have skewed are distribution to the right quite a bit and caused our model to over predict on lower claims and under predict on larger claims values.
* To remedy this, you could remove the extreme event and try and only focus on the aggregate flood claims.


## Conclusions

* Spark is extremely useful for handling large datasets
* In particular if you need to model something in that dataset
* As insurance datasets become more readily available, more and more data scientist will start to spark to analyse them in the industry



### --------------------------------------------------
# How run the analysis

1. Run the get_data_script.py file (this will take awhile)
    1. pass arguement for the path of where to download the zip to
    2. for example: python get_data_script.py "C:/Users/jmagr/Downloads/policy/" 
2. Run the unzip_csvs.py file
    1. pass argument for the path of where the zips are located and the output folder
    2. for example: python unzip_csvs.py 
3. Run the data_prep_script.py file
    1. pass arguement for the State you want to filter the datasets for
    2. for example: python data_prep_script.py NY
4. Take the outputted files and upload them to databricks - claims and policies
    1. use the write_ notebooks to import the datasets into permanent storage in your databricks account
5. Run the NFIP_Analysis notebook 

## Useful links

[Claims GLM Spark](http://people.stat.sc.edu/haigang/GLM_in_spark.html)
[NFIP Policy Data Dictonary](https://www.nap.edu/read/21848/chapter/5#51)
[NFIP Policy data](https://www.fema.gov/media-library/assets/documents/180376)
[NFIP Claims data](https://www.fema.gov/media-library/assets/documents/180374)
[Read Me shortcuts](https://github.com/tchapi/markdown-cheatsheet/blob/master/README.md)

# Understanding the data

## THE NFIP POLICY DATABASE

[NFIP Policy Data Dictonary](https://www.nap.edu/read/21848/chapter/5#51)

### Depending on the type of structure being insured, the NFIP uses three different forms for flood insurance policy applications:

* Dwelling forms are used for homeowners, residential renters, or owners of residential buildings that contain one to four units.
General property forms are used for owners of residential buildings with five or more units, as well as for owners or lessees of nonresidential buildings or units.
* Lastly, residential condo building association forms are provided to residential condo associations on behalf of the association and their unit owners (FEMA, 2014).


* NFIP tracks policies by the individual, rather than by the property, unique policy numbers are assigned to each policyholder and property holder. When a policyholder moves, a new policy number is assigned. Some aspects of the policy are protected by FEMA under privacy concerns, such as names and addresses, and are not available to the public. Other general policy information within the NFIP policy database includes

### policy status (active, canceled, etc.);
* number of policy terms (number of years the policy is effective: 1 year, 3 years, etc.);
* whether the policy was required for disaster assistance, and if so, by which agency;
* company code of the WYO company responsible for the policy (where applicable);
* whether the policy is for a single-family or multifamily property; and
* whether it is for a residential or nonresidential property.
* Location. Several attributes related to the spatial location of the insured property can be found in the NFIP policy database, including the latitude and longitude coordinates of the property; the property address, city, state, and zip code; the FEMA region; and which Census block (and block group) the property falls within. Information on how accurate the horizontal geocoding is for the property is also provided so the user knows how well the policy is located. The NFIP application does not contain the latitude and longitude coordinates of the property. This information is generated by FEMA using outside firms to geocode the property address. In addition, FEMA includes the attributes from the FIRMs as part of the policy database. The NFIP community and county that the insured structure is located within, as well as the map panel number and flood zone, as obtained from the FIRMs, are also provided.

### Chosen Coverage. 
* Insurance deductible and coverage amounts for both the property and the contents are included within the policy database, as are premiums. Policy endorsement dates, original effective dates (for rollover policies), current effective dates, and expiration dates are also provided.

### Premiums/Policy Type
Several attributes within the NFIP policy database are utilized for the insurance premium calculations. Some of these elements include

* whether it is a new policy or a renewed policy;
* what flood zone was used for rating the policy;
* if the policy has a V zone (which are areas within the special flood hazard area (SFHA) with additional hazards associated with storm-induced waves) risk-factor rating, where a qualified professional assesses the building’s location, its support system, and its ability to withstand wind and wave action. If the professional certifies that the property has a lower risk of flood damage based on these three factors, then the property becomes eligible for a premium discount;
* whether it is a pre- or post-FIRM property;
* type of residence;
* whether the policy falls under any BW 2012 categories and, if so, which BW 2012 category it would fall under. Some examples of the BW 2012 categories include single-family nonprincipal residences, businesses, severe repetitive loss pre-FIRM subsidized properties, and multifamily residences;
* whether the property is in a Community Rating System (CRS) community, and if so, which CRS class that provides premium discounts to all homeowners in the community ranging from 5 percent (Class 9) to 45 percent (Class 1);
* the policy’s NFIP community program type (regular or emergency);
* the location of the contents within the structure; and
* any obstruction types/categories associated with the structure. Some of the factors used to establish the obstruction categories include the size of the structure (less than or greater than 300 square feet), whether the structure has breakaway walls, if the building has an enclosure or crawl space with proper openings, whether there is machinery or equipment and is it above or below the base flood elevation, whether there is an elevator and is it above or below the base flood elevation, and whether the building is elevated.
### Building Characteristics. 
* The policy dataset provides several building attributes that can be used to review and assess flood risk at the structure level. Some of these characteristics include when the structure was built, whether the property is in the course of construction, the number of units within the property, the number of floors in the building, the type of basement or enclosure it has (if any), and whether the building is elevated and/ or flood proofed.

### Elevation Data. 
The following fields are provided in the NFIP policy database, but the information within them is not fully populated for all policies:

* Base flood elevation (BFE) from the FIRMs
* Whether there is an elevation certificate for the property and, if so, what the diagram number is
* Elevation of the lowest floor
* Elevation difference between the BFE and the lowest floor
* Lowest adjacent grade


### Miscellaneous Attributes. 
There are a few other fields included within the NFIP policy database that do not necessarily fit within the categories mentioned above, but that may still be of value for an affordability analysis. These include

* whether the property is state owned,
* the federal policy fee,
* the community probation surcharge amount, and
* the insurance to value indicator.



