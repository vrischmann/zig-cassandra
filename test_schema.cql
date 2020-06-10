CREATE KEYSPACE IF NOT EXISTS foobar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS foobar.age_to_ids(
	age int,
	name text,
	ids set<tinyint>,
	PRIMARY KEY ((age))
);
