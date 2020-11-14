REGISTER /usr/lib/pig/piggybank.jar;

DEFINE remove_first_lines ( X, N ) RETURNS Y{
A = RANK $X;
$Y = FILTER A BY $0>$N;
};

casualties = LOAD 'projekt1/casualties-breakdown.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
    as(street:chararray, zip:int, i_pedestrians:int, k_pedestrians:int, i_cyclists:int, k_cyclists:int, i_motorists:int, k_motorists:int);

zips_boroughs = LOAD 'projekt1/zips-boroughs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
    as (zip_code:int, borough:chararray);

raw_casualties = remove_first_lines(casualties, 1);
raw_zips_boroughs = remove_first_lines(zips_boroughs, 1);

processed_casualties = FOREACH raw_casualties GENERATE street, zip, i_pedestrians, k_pedestrians,  i_pedestrians + k_pedestrians as pedestrians, i_cyclists, k_cyclists, i_cyclists + k_cyclists as cyclists, i_motorists, k_motorists, i_motorists + k_motorists as motorists;
processed_zips_boroughs = FOREACH raw_zips_boroughs GENERATE zip_code, borough;

casualties_with_boroughs = JOIN processed_casualties BY zip, processed_zips_boroughs BY zip_code;

casualties_in_manhattan = FILTER casualties_with_boroughs BY borough == 'MANHATTAN';

pedestrians_casualties_in_manhattan = ORDER casualties_in_manhattan BY pedestrians DESC;
cyclists_casualties_in_manhattan = ORDER casualties_in_manhattan BY cyclists DESC;
motorists_casualties_in_manhattan = ORDER casualties_in_manhattan BY motorists DESC; 

pedestrians_worst3_in_manthattan = LIMIT pedestrians_casualties_in_manhattan 3;
cyclists_worst3_in_manthattan = LIMIT cyclists_casualties_in_manhattan 3;
motorists_worst3_in_manthattan = LIMIT motorists_casualties_in_manhattan 3;

pedestrians_final = FOREACH pedestrians_worst3_in_manthattan GENERATE street, 'PEDESTRIANS' as person_type, k_pedestrians as killed, i_pedestrians as injured;
cyclists_final = FOREACH cyclists_worst3_in_manthattan GENERATE street, 'CYCLISTS' as person_type, k_cyclists as killed, i_cyclists as injured;
motrists_final = FOREACH motorists_worst3_in_manthattan GENERATE street, 'MOTORISTS' as person_type, k_motorists as killed, i_motorists as injured;

STORE pedestrians_final INTO 'pedestrians_result.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
STORE cyclists_final INTO 'cyclists_result.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
STORE motorists_final INTO 'motorists_result.csv' USING USING org.apache.pig.piggybank.storage.CSVExcelStorage();