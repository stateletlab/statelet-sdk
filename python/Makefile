PROTO_DIR = ../proto
OUT_DIR   = statelet

.PHONY: proto clean

proto:
	python3 -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=$(OUT_DIR) \
		--grpc_python_out=$(OUT_DIR) \
		$(PROTO_DIR)/statelet.proto
	@# Fix import path: grpc codegen uses bare `import statelet_pb2` which fails
	@# when the file lives inside the statelet package. Rewrite to relative import.
	sed -i '' 's/^import statelet_pb2/from statelet import statelet_pb2/' $(OUT_DIR)/statelet_pb2_grpc.py

clean:
	rm -f $(OUT_DIR)/statelet_pb2.py $(OUT_DIR)/statelet_pb2_grpc.py
