#pragma once

#include <stdint.h>

namespace rucksack
{
namespace constants
{
	enum constants
	{
		MagicNumber = 0x12345678
	};

    enum op_codes
    {
        ConnectionHeaderOp = 0x01,
        DataChunkOp = 0x02,
        OpCodeMax = 0x02
    };
}
// So a bag consists of a header than a big array of chunks labeled with a byte opcode then a uint32 size

// the chunk types include:
// DataChunks: 0x01
//   Contains serialized message info from a single topic
// ConnectionHeader: 0x02
//   Contains data about a topic 

// In order to speed up bag analysis and playback it is possible to re-order the files such that the connection headers are at the start

// Header at the start of each bag file
// gives basic data about the file
// also links to the message definitions
struct Header
{
	uint32_t magic_number;// Identifies this as a bag file
	uint32_t version;// Gives the format version. Right now only 1 is valid.
	uint64_t start_time;// The time the recording of this bag file began
};

// The header for each Chunk
struct BlockHeader
{
	uint8_t op_code;
	uint32_t length_bytes;
};

// describes the message type for DataChunks with the given connection_id
// op code: 0x01
struct ConnectionHeader
{
	BlockHeader header;

	uint32_t connection_id;// incrementing id given to this connection
	uint32_t hash;
	// then goes the topic name string
	// the type name string 
	// then the message definition
};

// indicates the start of a chunk of messages in the file
// op code: 0x02
struct DataChunk
{
    BlockHeader header;

	uint32_t connection_id;// the connection_id of the header with the message info
	uint64_t start_time;// start time of messages in this chunk
	uint64_t end_time;// end time of messages in this chunk
};

// prefixes each message in the bag
struct MessageHeader
{
	uint64_t time;
	uint32_t length;// size of the message
};
}
