PROTO_DIR  = ../proto
OUT_DIR    = statelet/proto
# statelet.proto carries no `option go_package` (it is shared with the other
# SDKs), so the import path is mapped here instead — without it protoc-gen-go
# refuses to generate.
GO_PKG     = github.com/stateletlab/statelet-sdk/go/statelet/proto
PROTO_MAP  = Mstatelet.proto=$(GO_PKG)

.PHONY: proto test clean

proto:
	@mkdir -p $(OUT_DIR)
	protoc -I$(PROTO_DIR) \
		--go_out=$(OUT_DIR) --go_opt=paths=source_relative --go_opt=$(PROTO_MAP) \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative --go-grpc_opt=$(PROTO_MAP) \
		$(PROTO_DIR)/statelet.proto

test: proto
	go vet ./...
	go test ./...

clean:
	rm -f $(OUT_DIR)/*.go
