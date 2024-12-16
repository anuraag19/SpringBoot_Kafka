# SpringBoot_Kafka

- corrected the topic name in the code, was incorrect during the interview
- Also Kafka issue was corrected, by running it via docker compose
  kafka:
  image: apache/kafka
  ports:
  - "9092:9092"
  restart: always