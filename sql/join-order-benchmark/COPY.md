```sh
yb-voyager import data file --export-dir /home/yugabyte/tmp \
        --data-dir /home/yugabyte/data/job \
        --target-db-host $HOST \
        --target-db-port 5433 \
        --target-db-user $USER \
        --target-db-password $PASSWORD \
        --target-db-name taqo \
        --file-table-map "aka_name.csv:aka_name,aka_title.csv:aka_title,cast_info.csv:cast_info,char_name.csv:char_name,comp_cast_type.csv:comp_cast_type,company_name.csv:company_name,company_type.csv:company_type,complete_cast.csv:complete_cast,info_type.csv:info_type,keyword.csv:keyword,kind_type.csv:kind_type,link_type.csv:link_type,movie_companies.csv:movie_companies,movie_info.csv:movie_info,movie_info_idx.csv:movie_info_idx,movie_keyword.csv:movie_keyword,movie_link.csv:movie_link,name.csv:name,person_info.csv:person_info,role_type.csv:role_type,title.csv:title" \
        --delimiter "," \
        --start-clean
```