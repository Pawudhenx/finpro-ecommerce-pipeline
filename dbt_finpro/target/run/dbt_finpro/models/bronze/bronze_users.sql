
  
    

    create or replace table `jcdeah-006`.`fauzan_finpro`.`bronze_users`
      
    
    

    
    OPTIONS()
    as (
      

select
    user_id,
    name,
    email,
    cast(created_date as timestamp) as created_at
from `jcdeah-006`.`fauzan_finpro`.`users`
    );
  