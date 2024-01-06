
#include <rucksack/rucksack.h>

#include <cstring>

#undef max

namespace rucksack
{
Sack::Sack() : f_(0)
{

}

Sack::~Sack()
{
	close();
}

bool Sack::open(const std::string& file)
{
	if (f_)
	{
		close();
	}

	// open the file and read in the header
	f_ = fopen(file.c_str(), "rb");

	if (!f_)
	{
		return false;
	}

	//read the header
	fread(&header_, sizeof(header_), 1, f_);

	// check the header
	if (header_.magic_number != rucksack::constants::MagicNumber)
	{
		//bad file

		fclose(f_);
		f_ = 0;

		return false;
	}

	if (header_.version < rucksack::constants::MinSupportedVersion || header_.version > rucksack::constants::CurrentVersion)
	{
		fclose(f_);
        f_ = 0;

		return false;
	}

	return true;
}

// returns the block id that we read in
char* Sack::read_block(char& out_opcode)
{
	rucksack::ChunkHeader bheader;
	if (fread(&bheader, 1, sizeof(bheader), f_) != sizeof(bheader))
	{
		return 0;
	}

	// if we hit either of these cases, we probably hit the end
	if (feof(f_) || bheader.op_code > rucksack::constants::OpCodeMax)
	{
		return 0;
	}

	out_opcode = bheader.op_code;

	// go back to start of chunk
	fseek(f_, -sizeof(bheader), SEEK_CUR);

	// read in the whole chunk
	char* chunk = new char[bheader.length_bytes];
	fread(chunk, bheader.length_bytes, 1, f_);

	// perform any in-place migrations
	if (bheader.op_code == rucksack::constants::op_codes::ConnectionHeaderOp && header_.version == 1)
	{
		auto ch = (ConnectionHeaderV1*)chunk;
		ch->hash = 0;// hash becomes flags, zero it out so we dont mess anything up
	}
	return chunk;
}

SackReader::~SackReader()
{
	close();
}

SackWriter::SackWriter() :
  f_(0)
{

}

SackWriter::~SackWriter()
{
    close();
}

bool SackWriter::create(const std::string& file, pubsub::Time start, uint32_t chunk_size)
{
	if (f_)
	{
		close();
	}

	// open the file and read in the header
	f_ = fopen(file.c_str(), "wb");

	if (!f_)
	{
		return false;
	}

    chunk_size_ = chunk_size;

	//write the header
	rucksack::Header header;
	header.magic_number = rucksack::constants::MagicNumber;
	header.start_time = start.usec;
	header.version = 1;
	fwrite(&header, sizeof(header), 1, f_);

	return true;
}

bool SackReader::open(const std::string& file)
{
	if (data_.is_open())
	{
		data_.close();
	}

	current_chunk_ = 0;
	current_offset_ = 0;
	return data_.open(file);
}

void SackReader::close()
{
	if (!data_.is_open())
	{
		return;
	}

	// free our message definitions
	for (auto& info : channels_)
	{
		ps_free_message_definition(&info.definition);
	}

	delete[] current_chunk_;
	data_.close();
}

const void* SackReader::read(rucksack::MessageHeader const *& out_hdr, SackChannelDetails const*& out_info)
{
	// check if we are currently in a chunk
	if (!current_chunk_)
	{
		if (!get_next_chunk())
		{
			return 0;// we hit the end
		}
	}

	// okay, now we have a chunk, read from it
	rucksack::DataChunk* chunk = (rucksack::DataChunk*)current_chunk_;

	if (chunk->connection_id >= channels_.size())
	{
		printf("ERROR: Got data chunk with out-of-range channel id!");
		return 0;
	}

	// todo maybe should use a map?
	SackChannelDetails* details = &channels_[chunk->connection_id];

	if (current_offset_ >= chunk->header.length_bytes)
	{
		// get new chunk, we hit the end
		delete[] current_chunk_;

		if (!get_next_chunk())
		{
			return 0;// we hit the end
		}

		chunk = (rucksack::DataChunk*)current_chunk_;
		details = &channels_[chunk->connection_id];
	}

	rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&current_chunk_[current_offset_];

	char* msg = &current_chunk_[current_offset_ + sizeof(rucksack::MessageHeader)];
	current_offset_ += hdr->length + sizeof(rucksack::MessageHeader);

	// setup other output
	out_hdr = hdr;
	out_info = details;

	return msg;
}

bool SackReader::get_next_chunk()
{
	// open our first chunk
	char op_code;
	while (current_chunk_ = data_.read_block(op_code))
	{
		if (op_code == rucksack::constants::ConnectionHeaderOp)
		{
			handle_connection_header(current_chunk_);
			delete[] current_chunk_;
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			// we got data!
			current_offset_ = sizeof(rucksack::DataChunk);// reset the offset
			return true;
		}
		else
		{
			printf("ERROR: Got unhandled chunk\n");
			delete[] current_chunk_;
		}
	}
	return false;
}

void SackReader::handle_connection_header(const char* chunk)
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

bool SackMigrator::Migrate(const std::string& dst, const std::string& src)
{
	Sack reader;
	if (!reader.open(src))
	{
		return false;
	}

	if (reader.get_version() >= rucksack::constants::CurrentVersion)
	{
		return false;// already good
	}

	FILE* fout = fopen(dst.c_str(), "wb");
	if (fout == 0)
	{
		return false;
	}

	// write header
	auto copy = reader.get_header();
	copy.version = rucksack::constants::CurrentVersion;
	fwrite(&copy, sizeof(rucksack::Header), 1, fout);

	char op_code;
	while (char* chunk_ptr = reader.read_block(op_code))
	{
		// now write out each chunk
		ChunkHeader* block = (ChunkHeader*)chunk_ptr;
		if (op_code == rucksack::constants::ConnectionHeaderOp)
		{
			// now convert the chunk and write it to the bag
			ConnectionHeader* old = (ConnectionHeader*)chunk_ptr;
			old->flags = 0;// hash was replaced with flags, clear it out

			fwrite(chunk_ptr, block->length_bytes, 1, fout);
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			// just write the chunk back to the bag
			fwrite(chunk_ptr, block->length_bytes, 1, fout);
		}
		free(chunk_ptr);
	}

	fclose(fout);

	return true;
}

}
