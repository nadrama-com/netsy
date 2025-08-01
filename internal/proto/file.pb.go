// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v5.29.3
// source: proto/file.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FileKind int32

const (
	FileKind_KIND_UNKNOWN  FileKind = 0
	FileKind_KIND_SNAPSHOT FileKind = 1
	FileKind_KIND_CHUNK    FileKind = 2
)

// Enum value maps for FileKind.
var (
	FileKind_name = map[int32]string{
		0: "KIND_UNKNOWN",
		1: "KIND_SNAPSHOT",
		2: "KIND_CHUNK",
	}
	FileKind_value = map[string]int32{
		"KIND_UNKNOWN":  0,
		"KIND_SNAPSHOT": 1,
		"KIND_CHUNK":    2,
	}
)

func (x FileKind) Enum() *FileKind {
	p := new(FileKind)
	*p = x
	return p
}

func (x FileKind) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FileKind) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_file_proto_enumTypes[0].Descriptor()
}

func (FileKind) Type() protoreflect.EnumType {
	return &file_proto_file_proto_enumTypes[0]
}

func (x FileKind) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FileKind.Descriptor instead.
func (FileKind) EnumDescriptor() ([]byte, []int) {
	return file_proto_file_proto_rawDescGZIP(), []int{0}
}

type FileCompression int32

const (
	FileCompression_COMPRESSION_UNKNOWN FileCompression = 0
	FileCompression_COMPRESSION_NONE    FileCompression = 1
	FileCompression_COMPRESSION_ZSTD    FileCompression = 2
)

// Enum value maps for FileCompression.
var (
	FileCompression_name = map[int32]string{
		0: "COMPRESSION_UNKNOWN",
		1: "COMPRESSION_NONE",
		2: "COMPRESSION_ZSTD",
	}
	FileCompression_value = map[string]int32{
		"COMPRESSION_UNKNOWN": 0,
		"COMPRESSION_NONE":    1,
		"COMPRESSION_ZSTD":    2,
	}
)

func (x FileCompression) Enum() *FileCompression {
	p := new(FileCompression)
	*p = x
	return p
}

func (x FileCompression) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FileCompression) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_file_proto_enumTypes[1].Descriptor()
}

func (FileCompression) Type() protoreflect.EnumType {
	return &file_proto_file_proto_enumTypes[1]
}

func (x FileCompression) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FileCompression.Descriptor instead.
func (FileCompression) EnumDescriptor() ([]byte, []int) {
	return file_proto_file_proto_rawDescGZIP(), []int{1}
}

type FileHeader struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	SchemaVersion uint32                 `protobuf:"varint,2,opt,name=schema_version,json=schemaVersion,proto3" json:"schema_version,omitempty"`
	Kind          FileKind               `protobuf:"varint,3,opt,name=kind,proto3,enum=netsy.FileKind" json:"kind,omitempty"`
	Compression   FileCompression        `protobuf:"varint,4,opt,name=compression,proto3,enum=netsy.FileCompression" json:"compression,omitempty"`
	RecordsCount  int64                  `protobuf:"varint,5,opt,name=records_count,json=recordsCount,proto3" json:"records_count,omitempty"`
	LeaderId      string                 `protobuf:"bytes,6,opt,name=leader_id,json=leaderId,proto3" json:"leader_id,omitempty"`
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	Crc           uint64                 `protobuf:"varint,1,opt,name=crc,proto3" json:"crc,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FileHeader) Reset() {
	*x = FileHeader{}
	mi := &file_proto_file_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FileHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileHeader) ProtoMessage() {}

func (x *FileHeader) ProtoReflect() protoreflect.Message {
	mi := &file_proto_file_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileHeader.ProtoReflect.Descriptor instead.
func (*FileHeader) Descriptor() ([]byte, []int) {
	return file_proto_file_proto_rawDescGZIP(), []int{0}
}

func (x *FileHeader) GetSchemaVersion() uint32 {
	if x != nil {
		return x.SchemaVersion
	}
	return 0
}

func (x *FileHeader) GetKind() FileKind {
	if x != nil {
		return x.Kind
	}
	return FileKind_KIND_UNKNOWN
}

func (x *FileHeader) GetCompression() FileCompression {
	if x != nil {
		return x.Compression
	}
	return FileCompression_COMPRESSION_UNKNOWN
}

func (x *FileHeader) GetRecordsCount() int64 {
	if x != nil {
		return x.RecordsCount
	}
	return 0
}

func (x *FileHeader) GetLeaderId() string {
	if x != nil {
		return x.LeaderId
	}
	return ""
}

func (x *FileHeader) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *FileHeader) GetCrc() uint64 {
	if x != nil {
		return x.Crc
	}
	return 0
}

type FileFooter struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	RecordsCrc    uint64                 `protobuf:"varint,2,opt,name=records_crc,json=recordsCrc,proto3" json:"records_crc,omitempty"`
	FirstRevision int64                  `protobuf:"varint,3,opt,name=first_revision,json=firstRevision,proto3" json:"first_revision,omitempty"`
	LastRevision  int64                  `protobuf:"varint,4,opt,name=last_revision,json=lastRevision,proto3" json:"last_revision,omitempty"`
	Crc           uint64                 `protobuf:"varint,8,opt,name=crc,proto3" json:"crc,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FileFooter) Reset() {
	*x = FileFooter{}
	mi := &file_proto_file_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FileFooter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileFooter) ProtoMessage() {}

func (x *FileFooter) ProtoReflect() protoreflect.Message {
	mi := &file_proto_file_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileFooter.ProtoReflect.Descriptor instead.
func (*FileFooter) Descriptor() ([]byte, []int) {
	return file_proto_file_proto_rawDescGZIP(), []int{1}
}

func (x *FileFooter) GetRecordsCrc() uint64 {
	if x != nil {
		return x.RecordsCrc
	}
	return 0
}

func (x *FileFooter) GetFirstRevision() int64 {
	if x != nil {
		return x.FirstRevision
	}
	return 0
}

func (x *FileFooter) GetLastRevision() int64 {
	if x != nil {
		return x.LastRevision
	}
	return 0
}

func (x *FileFooter) GetCrc() uint64 {
	if x != nil {
		return x.Crc
	}
	return 0
}

var File_proto_file_proto protoreflect.FileDescriptor

const file_proto_file_proto_rawDesc = "" +
	"\n" +
	"\x10proto/file.proto\x12\x05netsy\x1a\x1fgoogle/protobuf/timestamp.proto\"\xa1\x02\n" +
	"\n" +
	"FileHeader\x12%\n" +
	"\x0eschema_version\x18\x02 \x01(\rR\rschemaVersion\x12#\n" +
	"\x04kind\x18\x03 \x01(\x0e2\x0f.netsy.FileKindR\x04kind\x128\n" +
	"\vcompression\x18\x04 \x01(\x0e2\x16.netsy.FileCompressionR\vcompression\x12#\n" +
	"\rrecords_count\x18\x05 \x01(\x03R\frecordsCount\x12\x1b\n" +
	"\tleader_id\x18\x06 \x01(\tR\bleaderId\x129\n" +
	"\n" +
	"created_at\x18\a \x01(\v2\x1a.google.protobuf.TimestampR\tcreatedAt\x12\x10\n" +
	"\x03crc\x18\x01 \x01(\x04R\x03crc\"\x8b\x01\n" +
	"\n" +
	"FileFooter\x12\x1f\n" +
	"\vrecords_crc\x18\x02 \x01(\x04R\n" +
	"recordsCrc\x12%\n" +
	"\x0efirst_revision\x18\x03 \x01(\x03R\rfirstRevision\x12#\n" +
	"\rlast_revision\x18\x04 \x01(\x03R\flastRevision\x12\x10\n" +
	"\x03crc\x18\b \x01(\x04R\x03crc*?\n" +
	"\bFileKind\x12\x10\n" +
	"\fKIND_UNKNOWN\x10\x00\x12\x11\n" +
	"\rKIND_SNAPSHOT\x10\x01\x12\x0e\n" +
	"\n" +
	"KIND_CHUNK\x10\x02*V\n" +
	"\x0fFileCompression\x12\x17\n" +
	"\x13COMPRESSION_UNKNOWN\x10\x00\x12\x14\n" +
	"\x10COMPRESSION_NONE\x10\x01\x12\x14\n" +
	"\x10COMPRESSION_ZSTD\x10\x02B-Z+github.com/nadrama-com/netsy/internal/protob\x06proto3"

var (
	file_proto_file_proto_rawDescOnce sync.Once
	file_proto_file_proto_rawDescData []byte
)

func file_proto_file_proto_rawDescGZIP() []byte {
	file_proto_file_proto_rawDescOnce.Do(func() {
		file_proto_file_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_file_proto_rawDesc), len(file_proto_file_proto_rawDesc)))
	})
	return file_proto_file_proto_rawDescData
}

var file_proto_file_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_proto_file_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_proto_file_proto_goTypes = []any{
	(FileKind)(0),                 // 0: netsy.FileKind
	(FileCompression)(0),          // 1: netsy.FileCompression
	(*FileHeader)(nil),            // 2: netsy.FileHeader
	(*FileFooter)(nil),            // 3: netsy.FileFooter
	(*timestamppb.Timestamp)(nil), // 4: google.protobuf.Timestamp
}
var file_proto_file_proto_depIdxs = []int32{
	0, // 0: netsy.FileHeader.kind:type_name -> netsy.FileKind
	1, // 1: netsy.FileHeader.compression:type_name -> netsy.FileCompression
	4, // 2: netsy.FileHeader.created_at:type_name -> google.protobuf.Timestamp
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_proto_file_proto_init() }
func file_proto_file_proto_init() {
	if File_proto_file_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_file_proto_rawDesc), len(file_proto_file_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_proto_file_proto_goTypes,
		DependencyIndexes: file_proto_file_proto_depIdxs,
		EnumInfos:         file_proto_file_proto_enumTypes,
		MessageInfos:      file_proto_file_proto_msgTypes,
	}.Build()
	File_proto_file_proto = out.File
	file_proto_file_proto_goTypes = nil
	file_proto_file_proto_depIdxs = nil
}
