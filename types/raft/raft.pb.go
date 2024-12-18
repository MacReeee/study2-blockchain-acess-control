// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v3.19.4
// source: raft.proto

package raft

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ErrorCode int32

const (
	ErrorCode_SUCCESS           ErrorCode = 0 // 表示成功
	ErrorCode_TERM_MISMATCH     ErrorCode = 1 // 任期不匹配
	ErrorCode_LOG_INCONSISTENCY ErrorCode = 2 // 日志不一致
	ErrorCode_LOG_DUPLICATE     ErrorCode = 3 // 日志重复
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0: "SUCCESS",
		1: "TERM_MISMATCH",
		2: "LOG_INCONSISTENCY",
		3: "LOG_DUPLICATE",
	}
	ErrorCode_value = map[string]int32{
		"SUCCESS":           0,
		"TERM_MISMATCH":     1,
		"LOG_INCONSISTENCY": 2,
		"LOG_DUPLICATE":     3,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_raft_proto_enumTypes[0].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_raft_proto_enumTypes[0]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{0}
}

type LogEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term  int32  `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`   // 当前日志条目对应的任期号
	Data  []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`    // 日志条目的数据
	Index int32  `protobuf:"varint,3,opt,name=index,proto3" json:"index,omitempty"` // 日志条目的索引
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntry.ProtoReflect.Descriptor instead.
func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{0}
}

func (x *LogEntry) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *LogEntry) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *LogEntry) GetIndex() int32 {
	if x != nil {
		return x.Index
	}
	return 0
}

type AppendEntryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term          int32       `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`                  // 领导者的任期号
	LeaderAddress string      `protobuf:"bytes,2,opt,name=leaderAddress,proto3" json:"leaderAddress,omitempty"` // 领导者的地址，以便于跟随者重定向请求
	Entry         []*LogEntry `protobuf:"bytes,3,rep,name=entry,proto3" json:"entry,omitempty"`                 // 需要复制的日志条目（即将要发送给其他服务器的日志条目）
	PrevLogIndex  int32       `protobuf:"varint,4,opt,name=prevLogIndex,proto3" json:"prevLogIndex,omitempty"`  // 领导者的 prevLogIndex
	PrevLogTerm   int32       `protobuf:"varint,5,opt,name=prevLogTerm,proto3" json:"prevLogTerm,omitempty"`    // 领导者的 prevLogTerm
	LeaderCommit  int32       `protobuf:"varint,6,opt,name=leaderCommit,proto3" json:"leaderCommit,omitempty"`  // 领导者的 commitIndex
}

func (x *AppendEntryRequest) Reset() {
	*x = AppendEntryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntryRequest) ProtoMessage() {}

func (x *AppendEntryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntryRequest.ProtoReflect.Descriptor instead.
func (*AppendEntryRequest) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{1}
}

func (x *AppendEntryRequest) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntryRequest) GetLeaderAddress() string {
	if x != nil {
		return x.LeaderAddress
	}
	return ""
}

func (x *AppendEntryRequest) GetEntry() []*LogEntry {
	if x != nil {
		return x.Entry
	}
	return nil
}

func (x *AppendEntryRequest) GetPrevLogIndex() int32 {
	if x != nil {
		return x.PrevLogIndex
	}
	return 0
}

func (x *AppendEntryRequest) GetPrevLogTerm() int32 {
	if x != nil {
		return x.PrevLogTerm
	}
	return 0
}

func (x *AppendEntryRequest) GetLeaderCommit() int32 {
	if x != nil {
		return x.LeaderCommit
	}
	return 0
}

type AppendEntryResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term       int32     `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`                               // 当前的任期号，用于领导者更新自己
	Success    bool      `protobuf:"varint,2,opt,name=success,proto3" json:"success,omitempty"`                         // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	MatchIndex int32     `protobuf:"varint,3,opt,name=matchIndex,proto3" json:"matchIndex,omitempty"`                   // 跟随者日志中最后一个条目的索引
	ErrorCode  ErrorCode `protobuf:"varint,4,opt,name=errorCode,proto3,enum=raft.ErrorCode" json:"errorCode,omitempty"` // 错误码
}

func (x *AppendEntryResponse) Reset() {
	*x = AppendEntryResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntryResponse) ProtoMessage() {}

func (x *AppendEntryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntryResponse.ProtoReflect.Descriptor instead.
func (*AppendEntryResponse) Descriptor() ([]byte, []int) {
	return file_raft_proto_rawDescGZIP(), []int{2}
}

func (x *AppendEntryResponse) GetTerm() int32 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntryResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *AppendEntryResponse) GetMatchIndex() int32 {
	if x != nil {
		return x.MatchIndex
	}
	return 0
}

func (x *AppendEntryResponse) GetErrorCode() ErrorCode {
	if x != nil {
		return x.ErrorCode
	}
	return ErrorCode_SUCCESS
}

var File_raft_proto protoreflect.FileDescriptor

var file_raft_proto_rawDesc = []byte{
	0x0a, 0x0a, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x72, 0x61,
	0x66, 0x74, 0x22, 0x48, 0x0a, 0x08, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x12,
	0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65,
	0x72, 0x6d, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x22, 0xde, 0x01, 0x0a,
	0x12, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x24, 0x0a, 0x0d, 0x6c, 0x65, 0x61, 0x64, 0x65,
	0x72, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d,
	0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x24, 0x0a,
	0x05, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x72,
	0x61, 0x66, 0x74, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x05, 0x65, 0x6e,
	0x74, 0x72, 0x79, 0x12, 0x22, 0x0a, 0x0c, 0x70, 0x72, 0x65, 0x76, 0x4c, 0x6f, 0x67, 0x49, 0x6e,
	0x64, 0x65, 0x78, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x70, 0x72, 0x65, 0x76, 0x4c,
	0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x20, 0x0a, 0x0b, 0x70, 0x72, 0x65, 0x76, 0x4c,
	0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x70, 0x72,
	0x65, 0x76, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x12, 0x22, 0x0a, 0x0c, 0x6c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x0c, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x22, 0x92, 0x01,
	0x0a, 0x13, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63,
	0x65, 0x73, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x49, 0x6e, 0x64, 0x65,
	0x78, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x49, 0x6e,
	0x64, 0x65, 0x78, 0x12, 0x2d, 0x0a, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0f, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f,
	0x64, 0x65, 0x2a, 0x55, 0x0a, 0x09, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x12,
	0x0b, 0x0a, 0x07, 0x53, 0x55, 0x43, 0x43, 0x45, 0x53, 0x53, 0x10, 0x00, 0x12, 0x11, 0x0a, 0x0d,
	0x54, 0x45, 0x52, 0x4d, 0x5f, 0x4d, 0x49, 0x53, 0x4d, 0x41, 0x54, 0x43, 0x48, 0x10, 0x01, 0x12,
	0x15, 0x0a, 0x11, 0x4c, 0x4f, 0x47, 0x5f, 0x49, 0x4e, 0x43, 0x4f, 0x4e, 0x53, 0x49, 0x53, 0x54,
	0x45, 0x4e, 0x43, 0x59, 0x10, 0x02, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f, 0x47, 0x5f, 0x44, 0x55,
	0x50, 0x4c, 0x49, 0x43, 0x41, 0x54, 0x45, 0x10, 0x03, 0x32, 0x4a, 0x0a, 0x04, 0x52, 0x61, 0x66,
	0x74, 0x12, 0x42, 0x0a, 0x0b, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x18, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x72, 0x61, 0x66,
	0x74, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x0c, 0x5a, 0x0a, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72,
	0x61, 0x66, 0x74, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_raft_proto_rawDescOnce sync.Once
	file_raft_proto_rawDescData = file_raft_proto_rawDesc
)

func file_raft_proto_rawDescGZIP() []byte {
	file_raft_proto_rawDescOnce.Do(func() {
		file_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_raft_proto_rawDescData)
	})
	return file_raft_proto_rawDescData
}

var file_raft_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_raft_proto_goTypes = []any{
	(ErrorCode)(0),              // 0: raft.ErrorCode
	(*LogEntry)(nil),            // 1: raft.LogEntry
	(*AppendEntryRequest)(nil),  // 2: raft.AppendEntryRequest
	(*AppendEntryResponse)(nil), // 3: raft.AppendEntryResponse
}
var file_raft_proto_depIdxs = []int32{
	1, // 0: raft.AppendEntryRequest.entry:type_name -> raft.LogEntry
	0, // 1: raft.AppendEntryResponse.errorCode:type_name -> raft.ErrorCode
	2, // 2: raft.Raft.AppendEntry:input_type -> raft.AppendEntryRequest
	3, // 3: raft.Raft.AppendEntry:output_type -> raft.AppendEntryResponse
	3, // [3:4] is the sub-list for method output_type
	2, // [2:3] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_raft_proto_init() }
func file_raft_proto_init() {
	if File_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_raft_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*LogEntry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*AppendEntryRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_raft_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*AppendEntryResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_raft_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_raft_proto_goTypes,
		DependencyIndexes: file_raft_proto_depIdxs,
		EnumInfos:         file_raft_proto_enumTypes,
		MessageInfos:      file_raft_proto_msgTypes,
	}.Build()
	File_raft_proto = out.File
	file_raft_proto_rawDesc = nil
	file_raft_proto_goTypes = nil
	file_raft_proto_depIdxs = nil
}
