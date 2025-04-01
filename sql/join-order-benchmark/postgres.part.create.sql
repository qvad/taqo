CREATE TABLE aka_name (
                          id integer NOT NULL PRIMARY KEY,
                          person_id integer NOT NULL,
                          name text NOT NULL,
                          imdb_index character varying(12),
                          name_pcode_cf character varying(5),
                          name_pcode_nf character varying(5),
                          surname_pcode character varying(5),
                          md5sum character varying(32)
);

CREATE TABLE aka_title (
                           id integer NOT NULL PRIMARY KEY,
                           movie_id integer NOT NULL,
                           title text NOT NULL,
                           imdb_index character varying(12),
                           kind_id integer NOT NULL,
                           production_year integer,
                           phonetic_code character varying(5),
                           episode_of_id integer,
                           season_nr integer,
                           episode_nr integer,
                           note text,
                           md5sum character varying(32)
);

CREATE TABLE cast_info (
                           id integer NOT NULL,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note text,
                           nr_order integer,
                           role_id integer NOT NULL,
                           PRIMARY KEY (movie_id, id)
) PARTITION BY RANGE (movie_id);

CREATE TABLE cast_info_p01 PARTITION OF cast_info
  FOR VALUES FROM (MINVALUE) TO (500000);
CREATE TABLE cast_info_p02 PARTITION OF cast_info
  FOR VALUES FROM (500000) TO (1000000);
CREATE TABLE cast_info_p03 PARTITION OF cast_info
  FOR VALUES FROM (1000000) TO (1500000);
CREATE TABLE cast_info_p04 PARTITION OF cast_info
  FOR VALUES FROM (1500000) TO (1750000);
CREATE TABLE cast_info_p05 PARTITION OF cast_info
  FOR VALUES FROM (1750000) TO (1850000);
CREATE TABLE cast_info_p06 PARTITION OF cast_info
  FOR VALUES FROM (1850000) TO (2000000);
CREATE TABLE cast_info_p07 PARTITION OF cast_info
  FOR VALUES FROM (2000000) TO (2150000);
CREATE TABLE cast_info_p08 PARTITION OF cast_info
  FOR VALUES FROM (2150000) TO (2250000);
CREATE TABLE cast_info_p09 PARTITION OF cast_info
  FOR VALUES FROM (2250000) TO (2400000);
CREATE TABLE cast_info_p10 PARTITION OF cast_info
  FOR VALUES FROM (2400000) TO (MAXVALUE);


CREATE TABLE char_name (
                           id integer NOT NULL PRIMARY KEY,
                           name text NOT NULL,
                           imdb_index character varying(12),
                           imdb_id integer,
                           name_pcode_nf character varying(5),
                           surname_pcode character varying(5),
                           md5sum character varying(32)
);

CREATE TABLE comp_cast_type (
                                id integer NOT NULL PRIMARY KEY,
                                kind character varying(32) NOT NULL
);

CREATE TABLE company_name (
                              id integer NOT NULL PRIMARY KEY,
                              name text NOT NULL,
                              country_code character varying(255),
                              imdb_id integer,
                              name_pcode_nf character varying(5),
                              name_pcode_sf character varying(5),
                              md5sum character varying(32)
);

CREATE TABLE company_type (
                              id integer NOT NULL PRIMARY KEY,
                              kind character varying(32) NOT NULL
);

CREATE TABLE complete_cast (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL
);

CREATE TABLE info_type (
                           id integer NOT NULL PRIMARY KEY,
                           info character varying(32) NOT NULL
);

CREATE TABLE keyword (
                         id integer NOT NULL PRIMARY KEY,
                         keyword text NOT NULL,
                         phonetic_code character varying(5)
);

CREATE TABLE kind_type (
                           id integer NOT NULL PRIMARY KEY,
                           kind character varying(15) NOT NULL
);

CREATE TABLE link_type (
                           id integer NOT NULL PRIMARY KEY,
                           link character varying(32) NOT NULL
);

CREATE TABLE movie_companies (
                                 id integer NOT NULL PRIMARY KEY,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note text
);

CREATE TABLE movie_info (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info text NOT NULL,
                            note text,
                            PRIMARY KEY (movie_id, id)
) PARTITION BY RANGE (movie_id);

CREATE TABLE movie_info_p01 PARTITION OF movie_info
  FOR VALUES FROM (MINVALUE) TO (500000);
CREATE TABLE movie_info_p02 PARTITION OF movie_info
  FOR VALUES FROM (500000) TO (1000000);
CREATE TABLE movie_info_p03 PARTITION OF movie_info
  FOR VALUES FROM (1000000) TO (1500000);
CREATE TABLE movie_info_p04 PARTITION OF movie_info
  FOR VALUES FROM (1500000) TO (1750000);
CREATE TABLE movie_info_p05 PARTITION OF movie_info
  FOR VALUES FROM (1750000) TO (1850000);
CREATE TABLE movie_info_p06 PARTITION OF movie_info
  FOR VALUES FROM (1850000) TO (2000000);
CREATE TABLE movie_info_p07 PARTITION OF movie_info
  FOR VALUES FROM (2000000) TO (2150000);
CREATE TABLE movie_info_p08 PARTITION OF movie_info
  FOR VALUES FROM (2150000) TO (2250000);
CREATE TABLE movie_info_p09 PARTITION OF movie_info
  FOR VALUES FROM (2250000) TO (2400000);
CREATE TABLE movie_info_p10 PARTITION OF movie_info
  FOR VALUES FROM (2400000) TO (MAXVALUE);


CREATE TABLE movie_info_idx (
                                id integer NOT NULL PRIMARY KEY,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info text NOT NULL,
                                note text
);

CREATE TABLE movie_keyword (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
                            id integer NOT NULL PRIMARY KEY,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL
);

CREATE TABLE name (
                      id integer NOT NULL PRIMARY KEY,
                      name text NOT NULL,
                      imdb_index character varying(12),
                      imdb_id integer,
                      gender character varying(1),
                      name_pcode_cf character varying(5),
                      name_pcode_nf character varying(5),
                      surname_pcode character varying(5),
                      md5sum character varying(32)
);

CREATE TABLE person_info (
                             id integer NOT NULL PRIMARY KEY,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info text NOT NULL,
                             note text
);

CREATE TABLE role_type (
                           id integer NOT NULL PRIMARY KEY,
                           role character varying(32) NOT NULL
);

CREATE TABLE title (
                       id integer NOT NULL PRIMARY KEY,
                       title text NOT NULL,
                       imdb_index character varying(12),
                       kind_id integer NOT NULL,
                       production_year integer,
                       imdb_id integer,
                       phonetic_code character varying(5),
                       episode_of_id integer,
                       season_nr integer,
                       episode_nr integer,
                       series_years character varying(49),
                       md5sum character varying(32)
);

create index company_id_movie_companies on movie_companies(company_id);
create index company_type_id_movie_companies on movie_companies(company_type_id);
create index info_type_id_movie_info_idx on movie_info_idx(info_type_id);
create index info_type_id_movie_info on movie_info(info_type_id);
create index info_type_id_person_info on person_info(info_type_id);
create index keyword_id_movie_keyword on movie_keyword(keyword_id);
create index kind_id_aka_title on aka_title(kind_id);
create index kind_id_title on title(kind_id);
create index linked_movie_id_movie_link on movie_link(linked_movie_id);
create index link_type_id_movie_link on movie_link(link_type_id);
create index movie_id_aka_title on aka_title(movie_id);
create index movie_id_cast_info on cast_info(movie_id);
create index movie_id_complete_cast on complete_cast(movie_id);
create index movie_id_movie_companies on movie_companies(movie_id);
create index movie_id_movie_info_idx on movie_info_idx(movie_id);
create index movie_id_movie_keyword on movie_keyword(movie_id);
create index movie_id_movie_link on movie_link(movie_id);
create index movie_id_movie_info on movie_info(movie_id);
create index person_id_aka_name on aka_name(person_id);
create index person_id_cast_info on cast_info(person_id);
create index person_id_person_info on person_info(person_id);
create index person_role_id_cast_info on cast_info(person_role_id);
create index role_id_cast_info on cast_info(role_id);
