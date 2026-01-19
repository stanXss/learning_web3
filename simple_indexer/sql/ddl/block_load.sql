CREATE TABLE eth_main.block_load (
	block_number int8 NOT NULL,
	load_table varchar(32) not NULL,
	CONSTRAINT block_load_pkey PRIMARY KEY (load_table, block_number)
);
