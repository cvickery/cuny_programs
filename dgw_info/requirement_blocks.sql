-- The requirement_blocks table
drop table if exists new_requirement_blocks cascade;

create table new_requirement_blocks (
 institution       text   not null,
 requirement_id    text   not null,
 block_type        text,
 block_value       text,
 title             text,
 period_start      text,
 period_stop       text,
 school            text,
 degree            text,
 college           text,
 major1            text,
 major2            text,
 concentration     text,
 minor             text,
 liberal_learning  text,
 specialization    text,
 program           text,
 student_id        text,
 requirement_text  text,
 requirement_html  text default 'Not Available'::text,
 parse_tree        jsonb default '{}'::jsonb,
 hexdigest         text,
 PRIMARY KEY (institution, requirement_id));
