class SqlQueries:
    business_facts_table_insert = ("""
with business as (

    select
        business_id,
        name,
        city,
        state,
        stars,
        review_count,
        is_open
    from business

),

tip as (

    select
        distinct business_id,
        sum(compliment_count) as total_compliments
    from tip
    group by business_id

),

checkin as (
    select
        distinct business_id,
        count(business_id) as total_checkins
    from checkin
    group by business_id
),


ufos as (
    select
        distinct state,
        count(*) as total_ufos
    from ufos
    group by state

),


joined_1 as (

    select
        business.*,
        tip.total_compliments
    from business
    left join tip on business.business_id = tip.business_id

),

joined_2 as (

    select
        joined_1.*,
        checkin.total_checkins
    from joined_1
    left join checkin on joined_1.business_id = checkin.business_id

),

joined_3 as (
    select
        joined_2.*,
        ufos.total_ufos
    from joined_2
    left join ufos on ufos.state = joined_2.state

)

select * from joined_3 """)


    business_table_insert = ("""
        SELECT
            distinct business_id,
            name,
            city,
            state,
            latitude,
            longitude,
            stars,
            review_count,
            is_open
        FROM staging_business
    """)

    checkin_table_insert = ("""
        SELECT
            distinct business_id
        FROM staging_checkin
    """)


    tip_table_insert = ("""
        SELECT
            distinct user_id,
            business_id,
            date,
            compliment_count
        FROM staging_tip
    """)

    users_table_insert = ("""
        SELECT
            user_id,
            name,
            review_count,
            yelping_since,
            elite
        FROM staging_users
    """)

    ufos_table_insert = ("""
        SELECT
            date_occurance,
            city,
            state,
            country,
            shape,
            duration_seconds,
            latitude,
            latitude
        FROM staging_ufos
    """)
