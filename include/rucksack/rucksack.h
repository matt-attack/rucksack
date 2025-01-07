#pragma once

#include "datastructures.h"
#include <pubsub_cpp/Time.h>

#include <pubsub/Serialization.h>

#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include <memory>

#include <stdio.h>
#include <cstring>

#undef min
#undef max

// todo put elsewhere later
struct Writer
{
  std::vector<uint8_t> buf;
  
  void reset()
  {
    buf.clear();
  }
  
  void ubyte(uint8_t i)
  {
    buf.push_back(i);
  }
  
  void ushort(uint16_t i)
  {
    // todo
    buf.push_back(i);
    buf.push_back(i >> 8);
  }
  
  void uint(uint32_t i)
  {
    // todo
    buf.push_back(i);
    buf.push_back(i >> 8);
    buf.push_back(i >> 16);
    buf.push_back(i >> 24);
  }
  
  void ulong(uint64_t i)
  {
    // todo
    buf.push_back(i);
    buf.push_back(i >> 8);
    buf.push_back(i >> 16);
    buf.push_back(i >> 24);
    buf.push_back(i >> 32);
    buf.push_back(i >> 40);
    buf.push_back(i >> 48);
    buf.push_back(i >> 56);
  }
  
  void string(const std::string& str)
  {
    uint(str.length() + 1);
    bytes((const uint8_t*)str.c_str(), str.length() + 1);
  }
  
  void bytes(const uint8_t* data, uint32_t len)
  {
    // this is dumb, but fine for now
    for (int i = 0; i < len; i++)
      buf.push_back(data[i]);
  }
};

namespace rucksack
{
// wraps around a rucksack for low level parsing
// enables access at the chunk level
class Sack
{
public:
	FILE* f_;
private:

	rucksack::Header header_;
	
public:

	Sack();
	~Sack();

  // Opens a bag file with the given name for reading.
  // Returns true if successful
	bool open(const std::string& file);
	
	// returns 0 if no index was found
	char* read_index();
	
	// reads the chunk at the given offset
	char* read_chunk(uint64_t offset);

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

	inline uint32_t get_version()
	{
		return header_.version;
	}
};

class SackMigrator
{
public:

	static bool Migrate(const std::string& dst, const std::string& src);
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
	
	// index information
	std::vector<ChunkIndex> chunks_;// this shouldnt need sorting
	std::vector<MessageIndex> messages_;// needs sorting
	std::vector<std::vector<uint8_t>> connection_headers_;// a copy of each connection header to put in the index

	struct ChannelWriter
	{
		QueueChunk open_chunk;
		uint32_t id;// connection id
    uint32_t chunk_size;

		std::string topic;
		const ps_message_definition_t* def;
		
		std::vector<std::pair<uint64_t, uint32_t>> messages;
		
		SackWriter* writer;

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
				open_chunk.header.end_time = time.usec;
				open_chunk.header.connection_id = id;
				uint32_t buf_size = std::max<uint32_t>(size + sizeof(rucksack::MessageHeader), chunk_size);
				open_chunk.data = new char[buf_size];
				open_chunk.current_position = 0;
			}
			else
			{
				// check if we have enough space, if not push out old chunk
				// if we are past the chunk size, start a new one and push this one
				if (open_chunk.current_position + size + sizeof(rucksack::MessageHeader) >= chunk_size)
				{
					flush(f);

					// start new chunk (make sure its at least as big as this message)
					open_chunk.header.start_time = time.usec;
					open_chunk.header.end_time = time.usec;
					open_chunk.header.connection_id = id;
					uint32_t buf_size = std::max<uint32_t>(size + sizeof(rucksack::MessageHeader), chunk_size);
					open_chunk.data = new char[buf_size];
					open_chunk.current_position = 0;
					
					messages.clear();
				}
			}

			// update timestamps
			open_chunk.header.start_time = std::min(open_chunk.header.start_time, time.usec);
			open_chunk.header.end_time = std::max(open_chunk.header.end_time, time.usec);

      auto start = open_chunk.current_position;
      
			//first write the message header
			rucksack::MessageHeader header;
			header.length = size;
			header.time = time.usec;
			memcpy(open_chunk.data+open_chunk.current_position, &header, sizeof(header));
			open_chunk.current_position += sizeof(header);

			// then write the data
			memcpy(open_chunk.data+open_chunk.current_position, data, size);
			open_chunk.current_position += size;
			
			messages.push_back({time.usec, start});
    }

    // Saves the current chunk
    void flush(FILE* f)
    {
			// fill in the rest of the header
			open_chunk.header.header.op_code = rucksack::constants::DataChunkOp;
			open_chunk.header.header.length_bytes = open_chunk.current_position + sizeof(open_chunk.header);

			// write header
			auto chunk_offset = ftell(f);
			
			fwrite(&open_chunk.header, sizeof(open_chunk.header), 1, f);

			// write body
			fwrite(open_chunk.data, 1, open_chunk.current_position, f);

			delete[] open_chunk.data;
			
			ChunkIndex ci;
			ci.chunk_offset = chunk_offset;
			ci.num_messages = messages.size();
			ci.connection_id = id;
			writer->chunks_.push_back(ci);
			
			// add the messages to the writer
			for (const auto& msg: messages)
			{
			  MessageIndex m;
			  m.timestamp = msg.first;
			  m.chunk_index = writer->chunks_.size()-1;
			  m.message_offset = msg.second;
			  writer->messages_.push_back(m);
			}
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
    return write_message(topic, ps_get_msg_start(msg.data), msg.len, def, time);
  }

  // Writes a single already encoded message to the bag file
  // Returns if successful. Fails if there is a message definition mismatch for the topic.
  bool write_message(const std::string& topic, const void* msg, uint32_t msg_size, const ps_message_definition_t* def, pubsub::Time time = pubsub::Time::now())
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
      writer.writer = this;
      channels_[topic] = writer;
      iter = channels_.find(topic);

      // Now save it
         
            
      //printf("Saving channel for topic\n");
			rucksack::ConnectionHeader header;
			header.header.op_code = rucksack::constants::ConnectionHeaderOp;
			header.connection_id = writer.id;
			header.flags = 0;// todo allow filling this out?

			char buf[1500];
			int def_len = ps_serialize_message_definition(buf, def);

			int data_length = def_len + topic.length() + 1;
			header.header.length_bytes = data_length + sizeof(header);
			//fwrite(&header, sizeof(header), 1, f_);

			// then goes the topic name string
			//fwrite(topic.c_str(), 1, topic.length() + 1, f_);

			// then the message definition
			//fwrite(buf, def_len, 1, f_);
			
			// serialize it all to a buffer so we can copy it easily
			Writer w;
      w.bytes((uint8_t*)&header, sizeof(header));
      w.bytes((uint8_t*)topic.c_str(), topic.length() + 1);
      w.bytes((uint8_t*)buf, def_len);
      
      connection_headers_.push_back(w.buf);
			
			// write it all at once
			fwrite(w.buf.data(), w.buf.size(), 1, f_);
    }
    else if (iter->second.def->hash != def->hash)
    {
      return false;
    }

    // Add message to the chunk
    iter->second.write(f_, time, msg, msg_size);

    return true;
  }

	// Writes a null-terminated metadata string to the rucksack
	void write_metadata(const std::string& data)
	{
    // First write the header
		rucksack::Metadata mdata;
		mdata.header.op_code = rucksack::constants::MetadataOp;
		mdata.metadata_length = data.length() + 1;
		mdata.header.length_bytes = mdata.metadata_length + sizeof(mdata) - 1;
		fwrite(&mdata, sizeof(mdata) - 1, 1, f_);

		// Then write the actual metadata
		fwrite(data.c_str(), 1, data.length() + 1, f_);
	}

  // Closes the open bag file, writing any unfinished chunks
	void close()
	{
    if (f_ == 0)
    {
      return;
    }
    
    // Finish any current chunks
    for (auto& item: channels_)
    {
      item.second.flush(f_);
    }
      
    // Generate and write the index
    std::sort(messages_.begin(), messages_.end(), [](MessageIndex a, MessageIndex b) {
      return a.timestamp < b.timestamp;// todo is this the right order
    });
    
    int i = 0;
    for (auto msg: chunks_)
    {
      printf("Chunk: %i Channel: %i %i messages at %li\n", i, msg.connection_id, msg.num_messages, msg.chunk_offset);
      i++;
    }
    
    for (auto msg: messages_)
    {
      printf("Message: %li us chunk: %i offset: %i\n", msg.timestamp, msg.chunk_index, msg.message_offset);
    }
    
    // now write the index
    
    uint64_t index_offset = ftell(f_);
    
    ChunkHeader hdr;
    hdr.op_code = constants::IndexChunkOp;
    hdr.length_bytes = sizeof(hdr) + sizeof(ChunkIndex)*chunks_.size() + sizeof(MessageIndex)*messages_.size();
    for (auto& chdr: connection_headers_)
    {
      hdr.length_bytes += chdr.size();
    }
    hdr.length_bytes += 3*4;// add sizes
    
    fwrite(&hdr, sizeof(hdr), 1, f_);
    
    uint32_t num_chunks = chunks_.size();
    fwrite(&num_chunks, 4, 1, f_);
    fwrite(chunks_.data(), sizeof(ChunkIndex), chunks_.size(), f_);
    
    uint32_t num_msgs = messages_.size();
    fwrite(&num_msgs, 4, 1, f_);
    fwrite(messages_.data(), sizeof(ChunkIndex), messages_.size(), f_);
    
    // write connection headers
    uint32_t num_hdrs = connection_headers_.size();
    fwrite(&num_hdrs, 4, 1, f_);
    for (auto& hdr: connection_headers_)
    {
      fwrite(hdr.data(), hdr.size(), 1, f_);
    }
    
    // update header with the index position
    printf("index position: %li\n", index_offset);
    fseek(f_, 8, SEEK_SET);// todo maybe dont hardcode these numbers
    fwrite(&index_offset, sizeof(uint64_t), 1, f_);

    fclose(f_);
    f_ = 0;
	}
};

struct SackChannelDetails
{
	std::string topic;
	std::string type;
	ps_message_definition_t definition;
	bool latched;
};

struct SackIndex
{
  std::vector<MessageIndex> messages;
  std::vector<ChunkIndex> chunks;
};

class SackIndexedReader
{
  Sack data_;
  SackIndex index_;
  
  std::map<int, std::unique_ptr<char[]>> chunk_cache_;

  int msg_idx_ = 0;
  
  std::vector<SackChannelDetails> channels_;
public:

  SackIndexedReader() {}
  
  bool open(const std::string& file)
  {
    if (data_.is_open())
    {
      data_.close();
      index_.messages.clear();
      index_.chunks.clear();
    }
    
    data_.open(file);
    
    auto index = data_.read_index();
    if (index == 0)
    {
      printf("ERROR: no index found!\n");
      data_.close();
      return false;
    }
    
    // read the index into a better structure
    auto data = (IndexChunk*)index;
    for (int i = 0; i < data->num_chunks; i++)
    {
      index_.chunks.push_back(data->chunk_offsets[i]);
    }
    
    auto msgs_start = index + sizeof(ChunkHeader) + 4 + sizeof(ChunkIndex)*data->num_chunks;
    uint32_t num_messages = *(uint32_t*)msgs_start;
    auto msgs = (MessageIndex*)(msgs_start+4);
    for (int i = 0; i < num_messages; i++)
    {
      index_.messages.push_back(msgs[i]);
    }
    
    // finally read our messages headers
    
    int i = 0;
    for (auto msg: index_.chunks)
    {
      printf("Chunk: %i Channel: %i %i messages at %li\n", i, msg.connection_id, msg.num_messages, msg.chunk_offset);
      i++;
    }
    
    for (auto msg: index_.messages)
    {
      printf("Message: %li us chunk: %i offset: %i\n", msg.timestamp, msg.chunk_index, msg.message_offset);
    }
    
    //handle each connection header
    auto hdrs_start = msgs_start + 4 + sizeof(MessageIndex)*num_messages;
    uint32_t num_hdrs = *(uint32_t*)hdrs_start;
    auto hdrs = hdrs_start + 4;
    for (int i = 0; i < num_hdrs; i++)
    {
      handle_connection_header(hdrs);
      hdrs += 0;// todo
    }
    
    delete[] index;
    
    return true;
  }
  
  void close()
  {
    data_.close();
  }
  
  int chunk_to_close_ = -1;
  const void* read(rucksack::MessageHeader const*&out_hdr, SackChannelDetails const*& out_info)
  {
    // just go through the index
    if (msg_idx_ >= index_.messages.size())
    {
      return 0;
    }
    
    if (chunk_to_close_ != -1)
    {
      chunk_cache_.erase(chunk_to_close_);
      chunk_to_close_ = -1;
    }

    auto message = index_.messages[msg_idx_];
    
    char* current_chunk;
    auto iter = chunk_cache_.find(message.chunk_index);
    if (iter == chunk_cache_.end())
    {
      // not cached, load it
      current_chunk = data_.read_chunk(index_.chunks[message.chunk_index].chunk_offset);
      chunk_cache_.emplace(message.chunk_index, current_chunk);
    }
    else
    {
      current_chunk = iter->second.get();
    }
    printf("%i chunks open\n", chunk_cache_.size());

	  // okay, now we have a chunk, read from it
	  rucksack::DataChunk* chunk = (rucksack::DataChunk*)current_chunk;

	  if (chunk->connection_id >= channels_.size())
	  {
		  printf("ERROR: Got data chunk with out-of-range channel id!");
		  return 0;
	  }

	  // todo maybe should use a map?
	  auto current_offset = message.message_offset;
	  SackChannelDetails* details = &channels_[chunk->connection_id];

	  //if (current_offset >= chunk->header.length_bytes)
	  //{
	  //  printf("invalid offset\n");
		//  throw 7;
	  //}

	  rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&current_chunk[current_offset + sizeof(DataChunk)];

	  char* msg = &current_chunk[current_offset + sizeof(rucksack::MessageHeader) + sizeof(DataChunk)];

	  // setup other output
	  out_hdr = hdr;
	  out_info = details;
	  
	  msg_idx_++;
	  
	  if (hdr->time == chunk->end_time)
	  {
	    chunk_to_close_ = message.chunk_index;
	  }
	
    return msg;
  }
  
  void handle_connection_header(const char* chunk)
	{
	  rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk;

	  // read in the details about this topic/connection
	  const char* topic = &chunk[sizeof(rucksack::ConnectionHeader)];
	  //const char* type = &chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1];

	  // todo handle duplicate message definitions/channels

	  // insert this into our header list
	  if (header->connection_id >= channels_.size())
	  {
		  channels_.resize(header->connection_id + 1);
	  }

	  ps_message_definition_t def;
	  ps_deserialize_message_definition(&chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1], &def);

	  SackChannelDetails details;
	  details.definition = def;
	  details.topic = topic;
	  details.type = def.name;
	  details.latched = ((header->flags & rucksack::constants::CHFLAG_LATCHED) > 0);
	  channels_[header->connection_id] = details;
  }
};

class SackReader
{
	std::vector<SackChannelDetails> channels_;

	Sack data_;

	char* current_chunk_;
	uint64_t current_offset_;
public:

	~SackReader();

  // Opens a bag file with the given name.
  // Returns true if successful
	bool open(const std::string& file);

  // Closes the file.
	void close();

	inline uint32_t get_version()
	{
		return data_.get_header().version;
	}

  // Note that this does not read in time order. It reads out entire chunks at a time (same message).
	const void* read(rucksack::MessageHeader const *& out_hdr, SackChannelDetails const*& out_info);

	// Get's the header from the loaded file
	inline const rucksack::Header& get_header()
	{
		return data_.get_header();
	}

private:

	bool get_next_chunk();
	void handle_connection_header(const char* chunk);
};
}
