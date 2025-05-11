-- last load lookup query
select * from [dbo].[water_table]

-- current load lookup query
select max(Date_ID) as max_date from [dbo].[source_cars_data]

-- copy to data lake query
select * from [dbo].[source_cars_data] 
where Date_ID > '@{activity('last_load').output.value[0].last_load}' and Date_ID <= '@{activity('current_load').output.value[0].max_date}'

-- update last load variable in stored procedure
-- last_load = '@{activity('current_load').output.value[0].max_date}'