create table neworder (
   no_o_id integer not null,
   no_d_id integer not null,
   no_w_id integer not null,
   primary key (no_w_id,no_d_id,no_o_id)
);

create table order (
   o_id integer not null,
   o_d_id integer not null,
   o_w_id integer not null,
   o_entry_d char(2) not null,
   primary key (o_w_id,o_d_id,o_id)
);

