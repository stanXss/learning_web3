create table eth_main.erc20_transfers
(
id serial primary key,
blockchain text not null,
token_address BYTEA not null,
tx_hash BYTEA not null,
log_index bigint not null,
block_number bigint not null,
block_time timestamp not null,
from_address BYTEA not null,
to_address BYTEA not null,
amount_raw numeric not null,
amount_decimals int not null
);

create unique index ix_bl_lg_tx on eth_main.erc20_transfers
(tx_hash, log_index);