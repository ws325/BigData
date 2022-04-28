def drop_data(database: str, tables: list):
    #database - nazwa bazydanych zawierajacej tabele do usuniecia
    #tables - lista tabeli do usuniecia

    if not database or not tables:
        raise ValueError

    for table in tables:
        spark.sql(f"DROP table {database}.{table}")