-- liquibase formatted sql

-- changeset markusk:1588371740142-1
alter table article_location
    drop constraint article_location_articles_article_id_fk;

alter table article_location
    add constraint article_location_articles_article_id_fk
        foreign key (article_id) references articles
            on delete cascade;
--rollback alter table article_location drop constraint article_location_articles_article_id_fk;
--rollback alter table article_location add constraint article_location_articles_article_id_fk foreign key (article_id) references articles;

-- changeset markusk:1588371740142-2
alter table article_location
    drop constraint article_location_locations_uuid_fk;

alter table article_location
    add constraint article_location_locations_uuid_fk
        foreign key (location_uuid) references locations
            on delete cascade;
--rollback alter table article_location drop constraint article_location_locations_uuid_fk;
--rollback alter table article_location add constraint article_location_locations_uuid_fk foreign key (location_uuid) references locations;

-- changeset markusk:1588371740142-3
alter table article_topic
    drop constraint article_topic_articles_article_id_fk;

alter table article_topic
    add constraint article_topic_articles_article_id_fk
        foreign key (article_id) references articles
            on delete cascade;
-- rollback alter table article_topic drop constraint article_topic_articles_article_id_fk;
-- rollback alter table article_topic add constraint article_topic_articles_article_id_fk foreign key (article_id) references articles;


-- changeset markusk:1588371740142-4
alter table article_topic
    drop constraint article_topic_topics_uuid_fk;

alter table article_topic
    add constraint article_topic_topics_uuid_fk
        foreign key (topic_uuid) references topics
            on delete cascade;
--rollback alter table article_topic drop constraint article_topic_topics_uuid_fk;
--rollback alter table article_topic add constraint article_topic_topics_uuid_fk foreign key (topic_uuid) references topics;