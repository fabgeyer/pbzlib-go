
all: messages.pb.go messages.descr

%.pb.go: %.proto
	protoc -I=$(dir $<) $< --go_out=$(dir $@)

%.descr: %.proto
	protoc -I=$(dir $<) --include_imports --descriptor_set_out=$@ $<
