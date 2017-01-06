
## Limitations

1. Timestamp is when the record is read by Kafka, not when it is generated. 
1. Everything is done in the initial database. Can logical replication span databases or is the replication slot to a single database.