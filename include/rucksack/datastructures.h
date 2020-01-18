#pragma once

#include "../high_level_api/Node.h"
//#include "../msg/std_msgs__String.msg.h"

#include <string>

namespace rucksack
{
namespace constants
{
	enum constants
	{
		MagicNumber = 0x12345678
	};
}
// So a bag consists of a header than a big array of chunks labeled with a byte opcode then a uint32 size

// the chunk types include:
// DataChunks: 0x01
//   Contains serialized message info from a single topic
// ConnectionHeader: 0x02
//   Contains data about a topic 

// In order to speed up bag analysis and playback it is possible to re-order the files such that the connection headers are at the start

// indicates the start of a chunk of messages in the file
struct DataChunk
{
	uint8_t op_code;
	uint32_t length_bytes;

	uint32_t connection_id;
	uint64_t start_time;
	uint64_t end_time;
};

struct BlockHeader
{
	uint8_t op_code;
	uint32_t length_bytes;
};

struct ConnectionHeader
{
	uint8_t op_code;
	uint32_t length_bytes;

	uint32_t connection_id;// incrementing id given to this connection
	uint32_t hash;
	// then goes the topic name string
	// the type name string 
	// then the message definition
};




// prefixes each message in the bag
struct MessageHeader
{
	uint64_t time;
	uint32_t length;// size of the message
};

// gives basic data about the file
// also links to the message definitions
struct Header
{
	uint32_t magic_number;
	uint32_t version;
	uint64_t start_time;
};
}