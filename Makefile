default: testacc
docs:
	go generate ./...
lint:
	golangci-lint run
fmt:
	gofmt -s -w -e .
testacc: required-BUCKET_NAME required-CLUSTER_ID
	BUCKET_NAME=${BUCKET_NAME} CLUSTER_ID=${CLUSTER_ID} TF_ACC=1 go test -count=1 -parallel=4 -timeout 10m -v ./...
required-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi

.PHONY: testacc fmt lint required-% docs