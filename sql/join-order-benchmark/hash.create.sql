CREATE TABLE aka_name (
                          id integer NOT NULL,
                          person_id integer NOT NULL,
                          name text NOT NULL,
                          imdb_index character varying(12),
                          name_pcode_cf character varying(5),
                          name_pcode_nf character varying(5),
                          surname_pcode character varying(5),
                          md5sum character varying(32),
                          PRIMARY KEY (id HASH)
);

CREATE TABLE aka_title (
                           id integer NOT NULL,
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
                           md5sum character varying(32),
                           PRIMARY KEY (id HASH)
);

CREATE TABLE cast_info (
                           id integer NOT NULL,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note text,
                           nr_order integer,
                           role_id integer NOT NULL,
                           PRIMARY KEY (id HASH)
);

CREATE TABLE char_name (
                           id integer NOT NULL,
                           name text NOT NULL,
                           imdb_index character varying(12),
                           imdb_id integer,
                           name_pcode_nf character varying(5),
                           surname_pcode character varying(5),
                           md5sum character varying(32),
                           PRIMARY KEY (id HASH)
);

CREATE TABLE comp_cast_type (
                                id integer NOT NULL,
                                kind character varying(32) NOT NULL,
                                PRIMARY KEY (id HASH)
);

CREATE TABLE company_name (
                              id integer NOT NULL,
                              name text NOT NULL,
                              country_code character varying(255),
                              imdb_id integer,
                              name_pcode_nf character varying(5),
                              name_pcode_sf character varying(5),
                              md5sum character varying(32),
                              PRIMARY KEY (id HASH)
);

CREATE TABLE company_type (
                              id integer NOT NULL,
                              kind character varying(32) NOT NULL,
                              PRIMARY KEY (id HASH)
);

CREATE TABLE complete_cast (
                               id integer NOT NULL,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL,
                               PRIMARY KEY (id HASH)
);

CREATE TABLE info_type (
                           id integer NOT NULL,
                           info character varying(32) NOT NULL,
                           PRIMARY KEY (id HASH)
);

CREATE TABLE keyword (
                         id integer NOT NULL,
                         keyword text NOT NULL,
                         phonetic_code character varying(5),
                         PRIMARY KEY (id HASH)
);

CREATE TABLE kind_type (
                           id integer NOT NULL,
                           kind character varying(15) NOT NULL,
                           PRIMARY KEY (id HASH)
);

CREATE TABLE link_type (
                           id integer NOT NULL,
                           link character varying(32) NOT NULL,
                           PRIMARY KEY (id HASH)
);

CREATE TABLE movie_companies (
                                 id integer NOT NULL,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note text,
                                 PRIMARY KEY (id HASH)
);

CREATE TABLE movie_info (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info text NOT NULL,
                            note text,
                            PRIMARY KEY (id HASH)
);

CREATE TABLE movie_info_idx (
                                id integer NOT NULL,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info text NOT NULL,
                                note text,
                                PRIMARY KEY (id HASH)
);

CREATE TABLE movie_keyword (
                               id integer NOT NULL,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL,
                               PRIMARY KEY (id HASH)
);

CREATE TABLE movie_link (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL,
                            PRIMARY KEY (id HASH)
);

CREATE TABLE name (
                      id integer NOT NULL,
                      name text NOT NULL,
                      imdb_index character varying(12),
                      imdb_id integer,
                      gender character varying(1),
                      name_pcode_cf character varying(5),
                      name_pcode_nf character varying(5),
                      surname_pcode character varying(5),
                      md5sum character varying(32),
                      PRIMARY KEY (id HASH)
);

CREATE TABLE person_info (
                             id integer NOT NULL,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info text NOT NULL,
                             note text,
                             PRIMARY KEY (id HASH)
);

CREATE TABLE role_type (
                           id integer NOT NULL,
                           role character varying(32) NOT NULL,
                           PRIMARY KEY (id HASH)
);

CREATE TABLE title (
                       id integer NOT NULL,
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
                       md5sum character varying(32),
                       PRIMARY KEY (id HASH)
);

create index company_id_movie_companies on movie_companies(company_id HASH);
create index company_type_id_movie_companies on movie_companies(company_type_id HASH);
create index info_type_id_movie_info_idx on movie_info_idx(info_type_id HASH);
create index info_type_id_movie_info on movie_info(info_type_id HASH);
create index info_type_id_person_info on person_info(info_type_id HASH);
create index keyword_id_movie_keyword on movie_keyword(keyword_id HASH);
create index kind_id_aka_title on aka_title(kind_id HASH);
create index kind_id_title on title(kind_id HASH);
create index linked_movie_id_movie_link on movie_link(linked_movie_id HASH);
create index link_type_id_movie_link on movie_link(link_type_id HASH);
create index movie_id_aka_title on aka_title(movie_id HASH);
create index movie_id_cast_info on cast_info(movie_id HASH);
create index movie_id_complete_cast on complete_cast(movie_id HASH);
create index movie_id_movie_companies on movie_companies(movie_id HASH);
create index movie_id_movie_info_idx on movie_info_idx(movie_id HASH);
create index movie_id_movie_keyword on movie_keyword(movie_id HASH);
create index movie_id_movie_link on movie_link(movie_id HASH);
create index movie_id_movie_info on movie_info(movie_id HASH);
create index person_id_aka_name on aka_name(person_id HASH);
create index person_id_cast_info on cast_info(person_id HASH);
create index person_id_person_info on person_info(person_id HASH);
create index person_role_id_cast_info on cast_info(person_role_id HASH);
create index role_id_cast_info on cast_info(role_id HASH);
