DROP TABLE book if exists ;
create table book(
id bigint identity not null primary key,
title varchar(90),
author varchar(40),
isbn  varchar(13),
publisher  varchar(20),
publishedOn decimal
);