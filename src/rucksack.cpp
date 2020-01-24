
#include <rucksack/rucksack.h>

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
		return false;
	}

	return true;
}

// returns the block id that we read in
char* Sack::read_block(char& out_opcode)
{
	rucksack::BlockHeader bheader;
	if (fread(&bheader, 1, sizeof(bheader), f_) != sizeof(bheader))
	{
		return 0;
	}

	// if we hit either of these cases, we probably hit the end
	if (feof(f_) || bheader.op_code > 0x02)
	{
		return 0;
	}

	out_opcode = bheader.op_code;

	// go back to start of chunk
	fseek(f_, -sizeof(bheader), SEEK_CUR);

	// read in the whole chunk
	char* chunk = new char[bheader.length_bytes];
	fread(chunk, bheader.length_bytes, 1, f_);
	return chunk;
}

bool SackWriter::create(const std::string& file, pubsub::Time start)
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

	//write the header
	rucksack::Header header;
	header.magic_number = rucksack::constants::MagicNumber;
	header.start_time = start.usec;
	header.version = 1;
	fwrite(&header, sizeof(header), 1, f_);

	return true;
}

}