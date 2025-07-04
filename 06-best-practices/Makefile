# Makefile for automating test tasks for Homework 06

.PHONY: all test unit-test integration-test setup-s3 check-localstack clean help

# Default target
all: test

# Main test target - runs all tests
test: check-localstack setup-s3 unit-test integration-test
	@echo ""
	@echo "✅ All tests completed successfully!"

# Check if LocalStack is running and start it if needed
check-localstack:
	@echo "Checking if LocalStack is running..."
	@if ! curl -s http://localhost:4566 > /dev/null; then \
		echo "LocalStack is not running. Starting it now..."; \
		docker-compose up -d; \
		echo "Waiting for LocalStack to be ready..."; \
		sleep 3; \
	else \
		echo "✅ LocalStack is already running."; \
	fi

# Set up the S3 bucket
setup-s3:
	@echo ""
	@echo "Setting up S3 bucket for testing..."
	@cd homework06/tests && python setup_s3.py

# Run unit tests
unit-test:
	@echo ""
	@echo "Running unit tests..."
	@cd homework06/tests && python -m pytest test_batch_refactoring.py -v

# Run integration test
integration-test:
	@echo ""
	@echo "Running integration tests..."
	@cd homework06/tests && python integration_test.py

# Run just batch prediction without tests
run-batch:
	@echo ""
	@echo "Running batch prediction for Jan 2023..."
	@cd homework06 && python batch_refactoring.py 2023 1

# Clean up generated files
clean:
	@echo "Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete

# Show help
help:
	@echo "Available targets:"
	@echo "  make         - Run all tests (default)"
	@echo "  make test    - Run all tests"
	@echo "  make unit-test - Run only unit tests"
	@echo "  make integration-test - Run only integration tests"
	@echo "  make setup-s3 - Set up the S3 bucket"
	@echo "  make check-localstack - Check if LocalStack is running"
	@echo "  make clean - Remove Python cache files"
	@echo "  make help - Show this help"
