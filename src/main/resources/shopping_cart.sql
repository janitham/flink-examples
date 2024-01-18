drop table if exists order_items;
drop table if exists order_summary;
drop table if exists orders;
drop table if exists items;

create table orders(
	order_id BigInt,
	order_date Date,
	primary key (order_id)
);

create table items(
	item_id BigInt,
	unit_price Float,
	primary key (item_id)
);

create table order_items(
	order_id BigInt,
	item_id BigInt,
	quantity BigInt,
	primary key (order_id, item_id, quantity),
	foreign key (order_id) references orders(order_id),
	foreign key (item_id) references items(item_id)
	
);

create table order_summary(
	order_id BigInt,
	total Float,
	primary key (order_id)
);

insert into orders (order_id, order_date) values (1, '2023/01/01');
insert into orders (order_id, order_date) values (2, '2023/01/01');
insert into orders (order_id, order_date) values (3, '2023/01/02');

insert into items (item_id, unit_price) values (1, 100);
insert into items (item_id, unit_price) values (2, 200);
insert into items (item_id, unit_price) values (3, 300);
insert into items (item_id, unit_price) values (4, 400);

insert into order_items (order_id, item_id, quantity) values (1, 1, 1);
insert into order_items (order_id, item_id, quantity) values (1, 2, 2);
insert into order_items (order_id, item_id, quantity) values (1, 3, 3);
insert into order_items (order_id, item_id, quantity) values (2, 4, 4);
insert into order_items (order_id, item_id, quantity) values (2, 1, 5);
insert into order_items (order_id, item_id, quantity) values (3, 2, 6);