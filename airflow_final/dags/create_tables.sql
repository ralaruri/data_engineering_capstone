CREATE TABLE IF NOT EXISTS public.business_facts (
	business_id varchar(256) NOT NULL,
	name varchar(256),
	city varchar(256),
	state varchar(256),
	stars int4,
	review_count int4,
	is_open int4,
	total_compliments int4,
	total_stars int4,
	total_checkins int4,
	total_ufos int4

);

CREATE TABLE IF NOT EXISTS public.business (
	business_id varchar(256) NOT NULL,
	name varchar(256),
	city varchar(256),
	state varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0),
	stars FLOAT4,
	review_count int4,
	is_open int4
);

CREATE TABLE IF NOT EXISTS public.checkin (
	business_id varchar(256) NOT NULL
);


CREATE TABLE IF NOT EXISTS public.tip (
	user_id varchar(256) NOT NULL,
	business_id varchar(256),
	date timestamp,
	compliment_count int4
);

CREATE TABLE IF NOT EXISTS public.users (
	user_id varchar(256),
	name varchar(256),
	review_count int4,
	yelping_since timestamp,
	elite varchar(256)
);

CREATE TABLE IF NOT EXISTS public.ufos(
	date_occurance timestamp NOT NULL,
	city varchar(256),
	state varchar(256),
	country varchar(256),
	shape varchar(256),
	duration_seconds FLOAT4,
	latitude numeric(18,0),
	longitude numeric(18,0)
);



CREATE TABLE IF NOT EXISTS public.staging_business (
	business_id varchar(256) NOT NULL,
	name varchar(256),
	city varchar(256),
	state varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0),
	stars FLOAT4,
	review_count int4,
	is_open int4
);

CREATE TABLE IF NOT EXISTS public.staging_checkin (
	business_id varchar(256) NOT NULL
);


CREATE TABLE IF NOT EXISTS public.staging_tip (
	user_id varchar(256) NOT NULL,
	business_id varchar(256),
	date timestamp,
	compliment_count int4
);

CREATE TABLE IF NOT EXISTS public.staging_users (
	user_id varchar(256),
	name varchar(256),
	review_count int4,
	yelping_since timestamp,
	elite varchar(256)
);

CREATE TABLE IF NOT EXISTS public.staging_ufos(
	date_occurance timestamp NOT NULL,
	city varchar(256),
	state varchar(256),
	country varchar(256),
	shape varchar(256),
	duration_seconds FLOAT4,
	latitude numeric(18,0),
	longitude numeric(18,0)
);
