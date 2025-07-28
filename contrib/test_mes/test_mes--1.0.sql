create function hello_world() returns text AS $$
BEGIN
    RETURN 'hello, world';
END;
$$ LANGUAGE plpgsql;

--- https://postgrespro.ru/docs/postgresql/16/plpgsql-declarations