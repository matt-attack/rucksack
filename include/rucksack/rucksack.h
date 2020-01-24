#pragma once

#include "datastructures.h"
#include <pubsub_cpp/Time.h>

#include <pubsub/Serialization.h>

#include <string>
#include <map>

#include <stdio.h>

namespace rucksack
{
// wraps around a rucksack for low level parsing
class Sack
{
	FILE* f_;

	rucksack::Header header_;
public:
	Sack();

	~Sack();

	bool create(const std::string& file);

	bool open(const std::string& file);

	// returns the block id that we read in
	char* read_block(char& out_opcode);

	inline void close()
	{
		fclose(f_);
	}

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
		fclose(f_);
	}
};
}