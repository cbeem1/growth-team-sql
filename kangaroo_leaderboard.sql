---leaderboard table--- 

with user_points as (
select user_id
,username 
,email
-- adding a bio-- DONE
,trim(JSON_QUERY(data, '$.publicInfo.bio'),'"')
--points for adding a bio-- DONE
,case when trim(JSON_QUERY(data, '$.publicInfo.bio'),'"') is not null and length(trim(JSON_QUERY(data, '$.publicInfo.bio'),'"')) > 0  then 200 else 0 end as bio_points
--create your avatar-- where do i find this info?
--remix count--- DONE
,JSON_QUERY(data, '$.publicInfo.fanEditCount')  as remix_count
--points for remix count-- DONE
,cast(JSON_QUERY(data, '$.publicInfo.fanEditCount') as INT64)* 600  as remix_points
--Finish first episode-- 
--Finish first series 
--Unlock asset reward--
--Win writing competition overall-- (manual)
--Win competition for IP partner-- (manual)
--Get 50+ followers-- DONE 
--Follow Kangaroo on Insta-- (manual?)
--Complete Kangaroo diary study-- (manual)
--Logging into Kangaroo daily-- DONE
from `fanchise-b3f85.Kangaroo_cleaned.users`)

,follower_count as (
select user_id
        ,count(distinct(follower_id)) as follower_count
        ,case when count(distinct(follower_id))  >= 50 then 200 else 0 end as follower_points
from        
    (select 
        user_id
        ,trim(follower_id,'"') as follower_id
    from 
    (select 
        document_id as user_id
        ,JSON_EXTRACT_ARRAY(data, '$.followers') as followers
        
        from
        (
        SELECT 
                SPLIT(document_name, '/')[OFFSET(5)]  as collection
                ,*
        FROM `fanchise-b3f85.all_export.all_raw_latest`

        )  
    where
        collection = 'followingInfo')
    ,unnest(followers) follower_id with offset
    order by 1)
group by 1)

,daily_login as (
select 
    user_id
    ,count(distinct(event_date)) as login_days
    ,count(distinct(event_date)) * 50 as login_points
from 
    `fanchise-b3f85.Kangaroo_cleaned.agg_session`
where user_id is not null 
--and event_date >= 2022-04-01 (or launch date)
group by 1
order by 2 desc
)
,avatar as 
    (select 
        document_id as user_id
        ,data 
        from
        (
        SELECT 
                SPLIT(document_name, '/')[OFFSET(5)]  as collection
                ,*
        FROM `fanchise-b3f85.all_export.all_raw_latest`

        )  
    where
        collection = 'avatars'
    )
,first_episode as 
(select user_id
        ,case when sum(finised_flag) >= 1 then 600 else 0 end as first_ep_points
from        
(select 
        trim(JSON_QUERY(data, '$.userId'),'"') as user_id
        , trim(JSON_QUERY(data, '$.episodeId'),'"') episode_id
        , JSON_QUERY(data, '$.gameState') gamestate
        , case when JSON_QUERY(data, '$.endTime') is not null then 1 else 0 end as finised_flag 
        from
        (
        SELECT 
                SPLIT(document_name, '/')[OFFSET(5)]  as collection
                ,*
        FROM `fanchise-b3f85.all_export.all_raw_latest`

        )  
    where
        collection = 'episodeStates'  )       
group by 1)      

select 
    user.user_id
    ,user.username
    ,user.email
    ,current_datetime("America/Los_Angeles") as run_stamp
    ,ifnull(user.bio_points,0) as bio_points
    ,ifnull(user.remix_points,0) as remix_points
    ,ifnull(follower.follower_points,0) as follower_points
    ,ifnull(login.login_points,0) as login_points
    ,ifnull(first_episode.first_ep_points,0) as first_ep_points
    ,case when avatar.user_id is not null then 400 else 0 end as avatar_points
from 
    user_points as user 
left join
    follower_count as follower
on 
    user.user_id = follower.user_id
left join
    daily_login as login 
on 
    user.user_id = login.user_id
left join
    avatar 
on 
    user.user_id = avatar.user_id
left join
    first_episode 
on 
    user.user_id = first_episode.user_id