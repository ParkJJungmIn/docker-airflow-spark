```
psql -h localhost -p 5433 -U postgres postgres

CREATE TABLE public.sample_table (
    id serial PRIMARY KEY,
    key VARCHAR ( 50 ) NOT NULL,
    value VARCHAR ( 50 ) NOT NULL
);

SELECT
    table_name,
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'sample_table';
```