create database if not exists userdb;
use userdb;
create table if not exists ngrama(bigram string, year int, match_count int, volume_count int)
    row format delimited
    fields terminated by '\t'
    stored as textfile;

load data local inpath 'a.txt' overwrite into table ngrama;

create table if not exists ngramb(bigram string, year int, match_count int, volume_count int)
    row format delimited
    fields terminated by '\t'
    stored as textfile;

load data local inpath 'b.txt' overwrite into table ngramb;

drop table if exists combined;

create table combined as
    select unioned.bigram,unioned.year,unioned.match_count
    from(
        select a.bigram,a.year,a.match_count
        from ngrama a
        union all
        select b.bigram,b.year,b.match_count
        from ngramb b
    ) unioned;

drop table if exists ngrama;
drop table if exists ngramb;
drop table if exists year_group;

create table year_group as
    select bigram,year,sum(match_count) as match_count
    from combined
    group by bigram, year;

drop table if exists combined;
drop table if exists word_count;

create table word_count as
    select bigram,avg(match_count) as avg_count
    from year_group
    group by bigram;

drop table if exists year_group;

select * from word_count order by avg_count desc limit 20;


