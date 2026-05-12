iceberg:
	go tool iceberg

list_schema:
	go tool iceberg --catalog hadoop --warehouse /tmp/iceberg-warehouse schema default.my_table

list_files:
	go tool iceberg --catalog hadoop --warehouse /tmp/iceberg-warehouse files default.my_table