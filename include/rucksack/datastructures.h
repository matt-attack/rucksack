#pragma once

#include <stdint.h>

namespace rucksack
{
namespace constants
{
	enum constants
	{
		MagicNumber = 0x12345678,
		MinSupportedVersion = 1,
		CurrentVersion = 3,
	};

	enum op_codes
	{
		ConnectionHeaderOp = 0x01,
		DataChunkOp = 0x02,
		MetadataOp = 0x03,
		IndexChunkOp = 0x04,
		OpCodeMax = 0x04
	};

	enum connection_header_flags
	{
		CHFLAG_LATCHED = 1
	};
}

#pragma pack(push, 1)
// So a sack consists of a header than a big array of chunks labeled with a byte opcode then a uint32 size

// the chunk types include:
// DataChunks: 0x01
//   Contains serialized message info from a single topic
// ConnectionHeader: 0x02
//   Contains data about a topic 
// Metadata: 0x03 (v2 sacks only)
//   

// In order to speed up sack analysis and playback it is possible to re-order the files such that the connection headers are at the start

// Header at the start of each sack file
// gives basic data about the file
struct Header
{
	// These first two fields must always remain the same or else 
	uint32_t magic_number;// Identifies this as a sack file
	uint32_t version;// Gives the format version. Right now only 1 and 2 are valid.

	union {
	  // if version <=2
	  uint64_t start_time;// The time the recording of this sack file began
	  // if version >=3
	  uint64_t index_offset;// Offset in bytes from the start of the file to the index in the file, invalid if 0
	};
};

// The header for each Chunk
struct ChunkHeader
{
	uint8_t op_code;
	uint32_t length_bytes;
};

// describes the message type for DataChunks with the given connection_id
// op code: 0x01
struct ConnectionHeader
{
	ChunkHeader header;

	uint32_t connection_id;// incrementing id given to this connection
	uint32_t flags;// stores things like latched
	// then goes the topic name string
	// the type name string
	// then the message definition
};

// previous version of the connection header in v1 bags, used only for conversion purposes
struct ConnectionHeaderV1
{
	ChunkHeader header;

	uint32_t connection_id;// incrementing id given to this connection
	uint32_t hash;// message hash, unused
	// then goes the topic name string
	// the type name string
	// then the message definition
};

// indicates the start of a chunk of messages in the file
// op code: 0x02
struct DataChunk
{
	ChunkHeader header;

	uint32_t connection_id;// the connection_id of the header with the message info
	uint64_t start_time;// start time of messages in this chunk in microseconds
	uint64_t end_time;// end time of messages in this chunk in microseconds

	// A list of MessageHeaders with attached messages follows
};

// extra metadata about the sack, generally in json format
struct Metadata
{
	ChunkHeader header;

	uint32_t metadata_length;// length in bytes of the metadata, including any null-terminator
	char metadata[1];// metadata string: can be any length, but this is convenient for access
};

// prefixes each message in the sack
struct MessageHeader
{
	uint64_t time;
	uint32_t length;// size of the message
};

// indicates the start of a chunk of messages in the file

struct MessageIndex
{
  uint64_t timestamp; // timestamp of the message in microseconds
  uint32_t chunk_index;// index of the chunk this message is contained in
  uint32_t message_offset;// offset in bytes in the chunk to the message header
};

struct ChunkIndex
{
  uint64_t chunk_offset;// offset in bytes to chunk the in the file
  uint32_t connection_id;// id of the connection header
  uint32_t num_messages;// number of messages in this chunk
};

// op code: 0x04
// optional chunk with index data
struct IndexChunk
{
	ChunkHeader header;
	
	uint32_t num_chunks;
	ChunkIndex chunk_offsets[0];

  // message indicies given in time order
  //uint32_t num_indices;// count of messages indexed below
	//MessageIndex indices[0];
	
	//uint32_t num_connection_headers;// count of copied message headers
	// connection headers go here
};

#pragma pack(pop)
}
