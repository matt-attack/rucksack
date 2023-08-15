#pragma once

#include <stdint.h>

namespace rucksack
{
namespace constants
{
	enum constants
	{
		MagicNumber = 0x12345678,
		CurrentVersion = 2
	};

	enum op_codes
	{
		ConnectionHeaderOp = 0x01,
		DataChunkOp = 0x02,
		MetadataOp = 0x03,
		OpCodeMax = 0x03
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
	uint64_t start_time;// The time the recording of this sack file began
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

// indicates the start of a chunk of messages in the file
// op code: 0x02
struct DataChunk
{
	ChunkHeader header;

	uint32_t connection_id;// the connection_id of the header with the message info
	uint64_t start_time;// start time of messages in this chunk
	uint64_t end_time;// end time of messages in this chunk

	// A list of MessageHeaders with attached messages follows
};

// extra metadata about the sack, generally in json format
struct Metadata
{
	ChunkHeader header;

	uint32_t metadata_length;
	char metadata[1];// can be any length, but this is convenient for access
};

// prefixes each message in the sack
struct MessageHeader
{
	uint64_t time;
	uint32_t length;// size of the message
};
#pragma pack(pop)
}
