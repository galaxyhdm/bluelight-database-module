CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
create table if not exists articles
(
    article_id      text        not null
        constraint articles_pk
            primary key,
    title           text        not null,
    url             text        not null,
    release_time    timestamp   not null,
    fetch_time      timestamp   not null,
    file_hash       varchar(64) not null,
    article_content text
);

create table if not exists topics
(
    uuid  uuid default uuid_generate_v4() not null
        constraint topics_pk
            primary key,
    topic text                            not null
);

create unique index if not exists topics_uuid_uindex
    on topics (uuid);

create unique index if not exists topics_topic_uindex
    on topics (topic);

create table if not exists locations
(
    uuid      uuid    default uuid_generate_v4() not null
        constraint locations_pk
            primary key,
    location  text                               not null,
    latitude  double precision,
    longitude double precision,
    indexed   boolean default false
);

create unique index if not exists locations_uuid_uindex
    on locations (uuid);

create unique index if not exists locations_location_uindex
    on locations (location);

create table if not exists article_topic
(
    uuid       uuid default uuid_generate_v4() not null
        constraint article_topic_pk
            primary key,
    article_id text                            not null
        constraint article_topic_articles_article_id_fk
            references articles,
    topic_uuid uuid
        constraint article_topic_topics_uuid_fk
            references topics,
    constraint article_topic_pk_2
        unique (article_id, topic_uuid)
);

create unique index if not exists article_topic_uuid_uindex
    on article_topic (uuid);

create table if not exists article_location
(
    uuid          uuid default uuid_generate_v4() not null
        constraint article_location_pk
            primary key,
    article_id    text                            not null
        constraint article_location_articles_article_id_fk
            references articles,
    location_uuid uuid
        constraint article_location_locations_uuid_fk
            references locations,
    constraint article_location_pk_2
        unique (article_id, location_uuid)
);

create unique index if not exists article_location_uuid_uindex
    on article_location (uuid);