#pragma once

#include "datastructures.h"
#include <pubsub_cpp/Time.h>

#include <pubsub/Serialization.h>

#include <string>
#include <map>
#include <vector>

#include <stdio.h>

namespace rucksack
{
// wraps around a rucksack for low level parsing
// enables access at the chunk level
class Sack
{
	FILE* f_;

	rucksack::Header header_;
public:

	Sack();
	~Sack();

    // Opens a bag file with the given name for reading.
    // Returns true if successful
	bool open(const std::string& file);

    // Reads in the next chunk from the bag file.
	// Returns a copy of the block id that we read in, or zero if finished. (Delete when done)
	char* read_block(char& out_opcode);

	inline bool is_open()
	{
		return f_ ? true : false;
	}

	inline void close()
	{
        if (f_)
        {
		    fclose(f_);
            f_ = 0;
        }
	}

    // Gets the header for the active file
    // Returns the header of the currently open file
	inline const rucksack::Header& get_header()
	{
		return header_;
	}
};

class SackWriter
{
	FILE* f_;

	struct QueueChunk
	{
		rucksack::DataChunk header;
		char* data;
		uint32_t current_position;
	};

	struct ChannelWriter
	{
		QueueChunk open_chunk;
		uint32_t id;

		std::string topic;
		ps_message_definition_t* def;

		bool written;

		ChannelWriter()
		{
			written = false;
			open_chunk.data = 0;
			open_chunk.current_position = 0;
		}
	};

	std::map<std::string, ChannelWriter> channels_;
public:
	SackWriter();

	~SackWriter();

	bool create(const std::string& file, pubsub::Time start = pubsub::Time::now());

	template <class T>
	void write_message(const std::string& topic, const T& message, pubsub::Time time = pubsub::Time::now())
	{
		
	}

	inline void close()
	{
        if (f_)
        {
		    fclose(f_);
        }
	}
};

class SackReader
{
	struct ChannelDetails
	{
		std::string topic;
		std::string type;
		ps_message_definition_t definition;
	};
	std::vector<ChannelDetails> channels_;

	Sack data_;

	char* current_chunk_;
	unsigned int current_offset_;
public:

	~SackReader();

    // Opens a bag file with the given name.
    // Returns true if successful
	bool open(const std::string& file);

    // Closes the file.
	void close();

    // Note that this does not read in time order. It reads out entire chunks at a time (same message).
	const void* read(rucksack::MessageHeader const *& out_hdr, ps_message_definition_t const*& out_def);

private:

	bool get_next_chunk();
	void handle_connection_header(const char* chunk);
};
}
