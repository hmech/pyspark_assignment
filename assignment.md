# Opensignal data engineering technical assignment

Given a medium-biggish dataset, let's gain some insights into that data using Apache Spark.

We will be looking at New York taxi data that are sourced from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
We have taken 2 months (January and Febuary 2015) and converted this to parquet.

* [https://assets.opensignal.com/static/engineering/task/ny\_taxi\_test\_data.zip](https://assets.opensignal.com/static/engineering/task/ny_taxi_test_data.zip)


| column name           | type      | notes                                |
| --------------------- | ----      | ----------------------------------   |
| vendor_id             | int       |                                      |
| tpep\_pickup\_datetime  | timestamp |                                      |
| tpep\_dropoff\_datetime | timestamp |                                      |
| passenger_count       | int       |                                      |
| trip_distance         | double    |                                      |
| pickup_longitude      | double    |                                      |
| pickup_latitude       | double    |                                      |
| rate\_code\_id          | int       |                                      |
| store\_and\_forward     | string    |                                      |
| dropoff_longitude     | double    |                                      |
| dropoff_latitude      | double    |                                      |
| payment_type          | int       |                                      |
| fare_amount           | double    |                                      |
| extra                 | double    |                                      |
| mta_tax               | double    |                                      |
| tip_amount            | double    |                                      |
| tolls_amount          | double    |                                      |
| improvement_surcharge | double    |                                      |
| total_amount          | double    |                                      |
| pickup\_h3\_index       | string    | **h3 index in base 16 (HEX) format** |
| dropoff\_h3\_index      | string    | **h3 index in base 16 (HEX) format** |
| taxi_id               | int       |                                      |


We also have data describing the multiple Zones and Boroughs in New York - this is in csv format.


| column name           | notes                                       |
| --------------------- | ------------------------------------------- |
| h3_index              |  **h3 index in base 10 (Decimal) format**   |
| zone                  |                                             |
| borough               |                                             |
| location_id           |                                             |


Finally, we have some data that list the taxi drivers who would like to be removed from the data store.

| column name           | notes                                               |
| --------------------- | --------------------------------------------------- |
| taxi_id               | ids of taxi drivers who would like to be forgotten  |


The folder structure:

```
ny_taxi_test_data ->  
    ny_zones ->
        ny_taxi_zones.csv
    ny_taxi_rtbf ->
        tbf_taxies.csv
    ny_taxi ->
        _metadata
        _common_metadata
        part-00014-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00029-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00008-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00003-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00022-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00013-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00004-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00025-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00018-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00012-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00005-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00024-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00019-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00015-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00028-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00009-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00002-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00023-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00010-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00007-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00026-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00017-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00000-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00021-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00016-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00001-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00020-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00011-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00006-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
        part-00027-30e54693-ac79-4825-bccf-e717ef1d631b-c000.snappy.parquet
```

We will ask to you to perform some tasks on these data using Apache Spark.  

As part of the assignemnt, please send back to us:

1. a script that can be run in Spark. 

2.  the outputs of that script.

## The Tasks

1. Install Apache Spark (no particular requirement for the installation).

2. Create a script that can be sent to this local installation of spark using `spark-submit`.

3. The supplied data is quite big so it is expected that you'll need to run this job with `spark.driver.memory` of 4g to 8g.

4. Now we can process the data; include the ny_taxi data between the dates `2015-01-15` to `2015-02-15` using the `tpep_pickup_datetime` column.

5. Filter right to be forgotten taxi\_ids. Remove all rows that have a `taxi_id` that is in the ny\_taxi\_rtbf list.

6. Load geocoding data `ny_taxi_zones` into Spark. This may need to be processed before use.

7. Using the geocoding data (`ny_taxi_zones`) and the appropriate index column in the `ny_taxi` data, geocode each pickup location with zone and borough. We would like 2 new columns: `pickup_zone` and `pickup_borough`.

8. Using the geocoding data (`ny_taxi_zones`) and the appropriate index column in the `ny_taxi` data, geocode each dropoff location with zone and borough. We would like 2 new columns: `dropoff_zone` and `dropoff_borough`.

9. **Insight 1:** Calculate the average total fare for each `trip_distance` (trip distance rounded to 0 decimal places) and the number of trips. Order the output by trip\_distance.  
Write the output as a single csv with headers. The resulting output should have 3 columns: `trip_distance`, `average_total_fare` and `number_of_trips`. 

    | trip_distance           | average\_total\_fare | number\_of\_trips  |
    | ---------------------   | ----               | ---------------- |
    | 0                       | 12                 | 200020           |
    | 1                       | 20.8               | 330303           |
    | 2                       | 33.8               | 10010            |

10. Looking at the output of step 9, decide what would be an appropriate upper limit of `trip_distance` and rerun with this filtered.

11. Filter rows if any of the columns: `pickup_zone`, `pickup_borough`, `dropoff_zone` and `dropoff_borough` are null.

12. **Insight 2:** Total number of pickups in each zone.  
Write the output as a single csv with headers. The resulting output should have 2 columns: `zone` and `number_of_pickups`.

    | zone                  | number\_of\_pickups |
    | --------------------- | ----------------  |
    | Governor's Island     | 20                |
    | Corona                | 33                |

13. **Insight 3:** Total number of pickups in each borough.  
Write the output as a single csv with headers. The resulting output should have 2 columns: `borough` and `number_of_pickups`.

    | borough               | number\_of\_pickups |
    | --------------------- | ----------------  |
    | Manhattan             | 20993             |
    | Queens                | 33443             |

14. **Insight 4:** Total number of dropoffs, average total cost and average distance in each zone.  
Write the output as a single csv with headers. The resulting output should have 4 columns: `zone`, `number_of_dropoffs`, `average_total_fare` and `average_trip_distance`.

    | zone                  | number\_of\_dropoffs | average\_total\_fare     | average\_trip\_distance |
    | --------------------- | ----------------   | ---------------------  | --------------------- |
    | Governor's Island     | 20                 |  22.4                  | 5.6                   |
    | Corona                | 33                 |  11.2                  | 7.5                   |

15. **Insight 5:** Total number of dropoffs, average total cost and average distance in each borough.  
Write the output as a single csv with headers. The resulting output should have 4 columns: `borough`, `number_of_dropoffs`, `average_total_fare` and `average_trip_distance`.

    | borough               | number\_of\_dropoffs | average\_total\_fare     | average\_trip\_distance |
    | --------------------- | ----------------   | ---------------------  | --------------------- |
    | Manhattan             | 20876              |  22.4                  | 5.6                   |
    | Queens                | 33876              |  11.2                  | 7.5                   |

16. **Insight 6:** For each pickup zone calculate the top 5 dropoff zones ranked by number of trips.  
Write output as a single csv with headers. The resulting output should have 4 columns: `pickup_zone`, `dropoff_zone`, `number_of_dropoffs` and `rank`.
    
    | pickup_zone     | dropoff_zone            | number\_of\_dropoffs  | rank |
    | --------------- | ----------------------- | ------------------- | ---- |
    | Alphabet City   | East Village	        | 5685                | 1    |
    | Alphabet City   | Lower East Side	        | 2858                | 2    |
    | Alphabet City   | Gramercy                | 2779                | 3    |
    | Alphabet City   | Greenwich Village South | 2272                | 4    |
    | Alphabet City   | Union Sq                | 2164                | 5    |

17. **Insight 7:** Calculate the average number of trips per hour (using `tpep_pickup_datetime`).  
The resulting output should have 2 columns: `hour_of_day` and `average_trips`.
    
    | hour\_of\_day | average_trips |
    | ----------- | ------------- |
    | 0           | 14552.4375    |
    | 1           | 11027.74194   |
    | 2           | 8241          |
    | 3           | 5994.516129   |