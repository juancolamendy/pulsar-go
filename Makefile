.PHONY: run-producer
run-producer:
	go run producer/main.go

.PHONY: run-consumer
run-consumer:
	@echo "run-consumer"
	go run consumer/main.go
