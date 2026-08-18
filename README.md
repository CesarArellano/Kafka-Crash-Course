

# Kafka Crash Course
## List Kafka Topics
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders 
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```