with shopify_utm_data as (
    select coalesce(ot.order_id,ona.order_id) as order_id
           ,max(case when ot.key like '%utm_source%'
                     then ot.value
                     else null
                 end) as ot_utm_source
           ,max(case when coalesce(ona.name,'') like '%utm_source%' and ona.value is not null  
                     then (ona.value)
                     else null
                 end) as ona_utm_source
           ,max(case when ot.key = 'utm_medium'
                     then ot.value
                     else null
                 end) as ot_utm_medium
            ,max(case when coalesce(ona.name,'') = 'utm_medium' and ona.value is not null
                      then (ona.value)
                      else null
                  end) as ona_utm_medium
            ,max(case when ot.key = 'utm_campaign'
                      then ot.value
                      else null
                  end) as ot_utm_campaign
            ,max(case when coalesce(ona.name,'') = 'utm_campaign' and ona.value is not null
                      then (ona.value)
                      else null
                  end) as ona_utm_campaign
            ,max(case when ot.key = 'utm_term'
                      then ot.value
                      else null
                  end) as ot_utm_term
            ,max(case when coalesce(ona.name,'') = 'utm_term' and ona.value is not null
                      then (ona.value)
                      else null
                  end) as ona_utm_term
            ,max(case when ot.key = 'utm_content'
                      then ot.value
                      else null
                  end) as ot_utm_content
            ,max(case when coalesce(ona.name,'') = 'utm_content'  and ona.value is not null
                      then (ona.value)
                      else null
                  end) as ona_utm_content
      from {{ source('SHOPIFY','ORDER_URL_TAG') }} ot
      full outer join {{ source('SHOPIFY','ORDER_NOTE_ATTRIBUTE') }} ona
        on ot.order_id = ona.order_id
     group by 1
)
, order_utm_data as (
    select order_id
           ,case when ot_utm_source is not null
                          then ot_utm_source
                          else ona_utm_source
                       end as utm_source
           ,case when ot_utm_source is not null
                          then ot_utm_medium
                          else ona_utm_medium
                      end as utm_medium
           ,case when ot_utm_source is not null
                          then ot_utm_campaign
                          else ona_utm_campaign
                      end as utm_campaign
           ,case when ot_utm_source is not null
                          then ot_utm_term
                          else ona_utm_term
                     end as utm_term
           ,case when ot_utm_source is not null
                          then ot_utm_content
                          else ona_utm_content
                      end as utm_content
      from shopify_utm_data
)
select * 
  from order_utm_data
 where not (utm_source is null 
            and utm_medium is null
            and utm_campaign is null
            and utm_term is null
            and utm_content is null
            )