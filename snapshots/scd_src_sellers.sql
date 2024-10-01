{% snapshot scd_src_sellers %}
    {{
      config(
            target_schema='DEV',
            target_database='WELLBEFORE', 
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            merge_cols=['surrogate_key'],
      )
    }}

    SELECT * FROM WELLBEFORE.DEV.src_sellers 

{% endsnapshot %}

