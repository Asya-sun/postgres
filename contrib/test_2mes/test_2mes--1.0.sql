create function hello_2world() returns text AS $$
BEGIN
    RETURN 'hello, 2world';
END;
$$ LANGUAGE plpgsql;

--- https://postgrespro.ru/docs/postgresql/16/plpgsql-declarations