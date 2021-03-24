#pragma once

#include "datastructures.h"
#include <pubsub_cpp/Time.h>

#include <pubsub/Serialization.h>

#include <string>
#include <map>
#include <vector>

#include <stdio.h>
#include <cstring>

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

    uint32_t chunk_size_;

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
        uint32_t chunk_size;

		std::string topic;
		const ps_message_definition_t* def;

		ChannelWriter()
		{
			open_chunk.data = 0;
			open_chunk.current_position = 0;
		}

        void write(FILE* f, pubsub::Time time, const void* data, uint32_t size)
        {
			if (open_chunk.data == 0)
			{
				// we can allocate it (make sure its at least as big as this message)
				open_chunk.header.start_time = time.usec;
				open_chunk.header.connection_id = id;
				int buf_size = std::max<int>(size + sizeof(rucksack::MessageHeader), chunk_size);
				open_chunk.data = new char[buf_size];
				open_chunk.current_position = 0;
			}
			else
			{
				// check if we have enough space, if not push out old chunk
				// if we are past the chunk size, start a new one and push this one
				if (open_chunk.current_position + size + sizeof(rucksack::MessageHeader) >= chunk_size)
				{
					open_chunk.header.end_time = time.usec;

                    flush(f);

					// start new chunk (make sure its at least as big as this message)
					open_chunk.header.start_time = time.usec;
					open_chunk.header.connection_id = id;
					int buf_size = std::max<int>(size + sizeof(rucksack::MessageHeader), chunk_size);
					open_chunk.data = new char[buf_size];
					open_chunk.current_position = 0;
				}
			}

			//first write the message header
			rucksack::MessageHeader header;
			header.length = size;
			header.time = time.usec;
			memcpy(open_chunk.data+open_chunk.current_position, &header, sizeof(header));
			open_chunk.current_position += sizeof(header);

			// then write the data
			memcpy(open_chunk.data+open_chunk.current_position, data, size);
			open_chunk.current_position += size;
        }

        // Saves the current chunk
        void flush(FILE* f)
        {
			// fill in the rest of the header
			open_chunk.header.header.op_code = rucksack::constants::DataChunkOp;
			open_chunk.header.header.length_bytes = open_chunk.current_position + sizeof(open_chunk.header);

			// write header
			fwrite(&open_chunk.header, sizeof(open_chunk.header), 1, f);

			// write body
			fwrite(open_chunk.data, 1, open_chunk.current_position, f);

			delete[] open_chunk.data;
        }
	};

	std::map<std::string, ChannelWriter> channels_;
public:
	SackWriter();

	~SackWriter();

    // Creates and opens a bag file at the given location for writing
	bool create(const std::string& file,
                pubsub::Time start = pubsub::Time::now(),
                uint32_t chunk_size = 1024*1000);

    // Writes a single message to the bag file
	template <class T>
	void write_message(const std::string& topic, const T& message, pubsub::Time time = pubsub::Time::now())
	{
        const ps_message_definition_t* def = T::GetDefinition();
        ps_msg_t msg_enc = message.Encode();
	    write_message(topic, msg_enc, def, time);
        free(msg_enc.data);
	}

    // Writes a single already encoded message to the bag file
    // Returns if successful. Fails if there is a message definition mismatch for the topic.
    bool write_message(const std::string& topic, const ps_msg_t& msg, const ps_message_definition_t* def, pubsub::Time time = pubsub::Time::now())
    {
        // if we havent had this topic before, create a channel
        auto iter = channels_.find(topic);
        if (iter == channels_.end())
        {
            ChannelWriter writer;
            writer.def = def;
            writer.topic = topic;
            writer.id = channels_.size();
            writer.chunk_size = chunk_size_;
            channels_[topic] = writer;
            iter = channels_.find(topic);

            // Now save it
		    //printf("Saving channel for topic\n");
			rucksack::ConnectionHeader header;
			header.header.op_code = rucksack::constants::ConnectionHeaderOp;
			header.connection_id = writer.id;
			header.hash = def->hash;

			char buf[1500];
			int def_len = ps_serialize_message_definition(buf, def);

			int data_length = def_len + topic.length() + 1;
			header.header.length_bytes = data_length + sizeof(header);
			fwrite(&header, sizeof(header), 1, f_);

			// then goes the topic name string
			fwrite(topic.c_str(), 1, topic.length() + 1, f_);

			// then the message definition
			fwrite(buf, def_len, 1, f_);
        }
        else if (iter->second.def != def)
        {
            return false;
        }

        // Add to the channel
        iter->second.write(f_, time, ps_get_msg_start(msg.data), msg.len);

        return true;
    }

    // Closes the open bag file, writing any unfinished chunks
	void close()
	{
        if (f_)
        {
            // Finish any current chunks
            for (auto& item: channels_)
            {
                item.second.flush(f_);
            }

		    fclose(f_);
            f_ = 0;
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
