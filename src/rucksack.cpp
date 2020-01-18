
#include <pubsub/Node.h>
#include <pubsub/Subscriber.h>

#include <rucksack/datastructures.h>

#include <pubsub_cpp/arg_parse.h>
#include <pubsub_cpp/Time.h>

#undef max

void wait(ps_node_t* node)
{
	printf("Waiting for responses...\n\n");
	unsigned int start = GetTimeMs();
	while (ps_okay() && start + 3000 > GetTimeMs())
	{
		ps_node_wait(node, 100);
		ps_node_spin(node);
	}
}

struct QueueChunk
{
	rucksack::DataChunk header;
	char* data;
	uint32_t current_position;
};

std::mutex _chunk_mutex;
std::vector<QueueChunk> _finished_chunks;

struct Channel;
struct QueueChannel
{
	Channel* channel;
};
std::mutex _channel_mutex;
std::vector<QueueChannel> _finished_channels;

// a list of topics we end up wanting to subscribe to

struct TopicData
{
	std::string topic;
	std::string type;
	uint32_t hash;

	~TopicData()
	{
		//if (definition)
		//{
		//	delete definition;
		//}
	}

	// message definition?
	//ps_message_definition_t definition;
};
std::vector<TopicData> _topics;

struct Channel
{
	QueueChunk open_chunk;
	ps_sub_t sub;
	uint32_t id;

	TopicData topic;

	bool written;

	Channel()
	{
		written = false;
		open_chunk.data = 0;
		open_chunk.current_position = 0;
	}
};
std::vector<Channel*> _channels;
uint32_t _channel_id = 0;

void record(const std::string& file)
{
	// open the file
	FILE* f = fopen(file.c_str(), "wb");

	// first lets get a list of all topics, and then subscribe to them
	ps_node_t node;
	ps_node_init(&node, "rucksack", "", true);

	ps_node_system_query(&node);

	node.adv_cb = [](const char* topic, const char* type, const char* node, const ps_advertise_req_t* data)
	{
		// todo check if we already have the topic

		printf("Subscribing to %s..\n", topic);
		
		TopicData tdata;
		tdata.topic = topic;
		tdata.type = type;
		tdata.hash = data->type_hash;
		_topics.push_back(tdata);
		return;
	};

	// create the data saving thread
	std::thread saver([f]()
	{
		while (ps_okay())
		{
			// check for channels and save em if we have em
			std::vector<QueueChannel> ch_to_save;
			_channel_mutex.lock();
			std::swap(ch_to_save, _finished_channels);
			_channel_mutex.unlock();

			// now write each of the chunks to the file
			for (auto& chunk : ch_to_save)
			{
				//printf("Saving channel for topic\n");
				rucksack::ConnectionHeader header;
				header.op_code = 0x02;
				header.connection_id = chunk.channel->id;
				header.hash = chunk.channel->topic.hash;

				char buf[1500];
				int def_len = ps_serialize_message_definition(buf, &chunk.channel->sub.received_message_def);

				int data_length = def_len + chunk.channel->topic.type.length() + 1 + chunk.channel->topic.topic.length() + 1;
				header.length_bytes = data_length + sizeof(header);
				fwrite(&header, sizeof(header), 1, f);

				// then goes the topic name string
				fwrite(chunk.channel->topic.topic.c_str(), 1, chunk.channel->topic.topic.length() + 1, f);

				// the type name string 
				fwrite(chunk.channel->topic.type.c_str(), 1, chunk.channel->topic.type.length() + 1, f);

				// then the message definition
				fwrite(buf, def_len, 1, f);
			}

			// check for chunks and save em if we have em
			std::vector<QueueChunk> to_save;
			_chunk_mutex.lock();
			std::swap(to_save, _finished_chunks);
			_chunk_mutex.unlock();

			// now write each of the chunks to the file
			for (auto& chunk : to_save)
			{
				//printf("Saving chunk for topic %i\n", chunk.header.connection_id);
				// fill in the rest of the header
				chunk.header.op_code = 0x01;
				chunk.header.length_bytes = chunk.current_position + sizeof(chunk.header);

				// write header
				fwrite(&chunk.header, sizeof(chunk.header), 1, f);

				// write body
				fwrite(chunk.data, 1, chunk.current_position, f);

				delete[] chunk.data;
			}

			// check for connection headers and save em if we have em


			// todo warn if we are getting behind
			
			// todo block
			ps_sleep(1);
		}
	});

	wait(&node);

	printf("Starting recording...\n");

	//write the header
	rucksack::Header header;
	header.magic_number = rucksack::constants::MagicNumber;
	header.start_time = pubsub::Time::now().usec;
	header.version = 1;
	fwrite(&header, sizeof(header), 1, f);

	static int count = 0;
	// now subscribe to things!
	node.adv_cb = 0;// todo dont do this
	for (int i = 0; i < _topics.size(); i++)
	{
		struct ps_subscriber_options options;
		ps_subscriber_options_init(&options);

		options.skip = 0;
		options.queue_size = 0;
		options.want_message_def = true;
		options.allocator = 0;
		options.ignore_local = false;
		options.cb = [](void* message, unsigned int size, void* data, const ps_msg_info_t* info)
		{
			count++;
			//todo need to make sure we dont wildcard subscribe on the topic without checking that they are all the same time
			Channel* channel = (Channel*)data;
			// get and deserialize the messages
			if (channel->sub.received_message_def.fields == 0)
			{
				printf("WARN: got message but no message definition yet...\n");
				// queue it up, then print them out once I get it
				//todo_msgs.push_back({ message, *info });
			}
			else
			{
				//if we already have a channel open, then
				// todo check if we saved the channel, if not do that
				if (channel->written == false)
				{
					// now that we have message definition we can save the data in a chunk
					QueueChannel ch_data;
					ch_data.channel = channel;
					_channel_mutex.lock();
					_finished_channels.push_back(ch_data);
					_channel_mutex.unlock();
					channel->written = true;
				}

				auto& chunk = channel->open_chunk;
				//write it into our current chunk for this subcriber

				//check if this message is too big for the rest of the chunk, if it is, save what we have then start a new one thats at least the size of it
				pubsub::Time now = pubsub::Time::now();

				const int chunk_size = 1600;// todo change this to be a parameter and make it bigger
				if (chunk.data == 0)
				{
					// we can allocate it
					chunk.header.start_time = now.usec;
					chunk.header.connection_id = channel->id;
					int buf_size = std::max<int>(size + sizeof(rucksack::MessageHeader), chunk_size);
					chunk.data = new char[buf_size];
					chunk.current_position = 0;
				}
				else
				{
					// check if we have enough space, if not push out old chunk
					// if we are past the chunk size, start a new one and push this one
					if (chunk.current_position + size + sizeof(rucksack::MessageHeader) >= chunk_size)
					{
						chunk.header.end_time = now.usec;
						_chunk_mutex.lock();
						_finished_chunks.push_back(chunk);
						_chunk_mutex.unlock();

						// start new chunk
						chunk.header.start_time = now.usec;
						chunk.header.connection_id = channel->id;
						int buf_size = std::max<int>(size + sizeof(rucksack::MessageHeader), chunk_size);
						chunk.data = new char[buf_size];
						chunk.current_position = 0;
					}
				}

				//first write the message header
				rucksack::MessageHeader header;
				header.length = size;
				header.time = now.usec;
				memcpy(chunk.data+chunk.current_position, &header, sizeof(header));
				chunk.current_position += sizeof(header);

				// then write the data
				memcpy(chunk.data+chunk.current_position, message, size);
				chunk.current_position += size;

				// todo remove me, just here for testing
				//ps_deserialize_print(message, &channel->sub.received_message_def);
				//printf("-------------\n");

				free(message);
			}
		};

		Channel* new_channel = new Channel;
		new_channel->id = _channel_id++;
		new_channel->topic = _topics[i];

		options.cb_data = new_channel;
		ps_node_create_subscriber_adv(&node, _topics[i].topic.c_str(), 0, &new_channel->sub, &options);

		_channels.push_back(new_channel);
	}

	// spin away!
	while (ps_okay())
	{
		// todo blocking
		ps_node_spin(&node);
		ps_sleep(1);
	}

	saver.join();

	// now save out any open chunks and close channels
	pubsub::Time now = pubsub::Time::now();
	for (auto& ch : _channels)
	{
		if (ch->open_chunk.data)
		{
			// todo use last message time for end time
			auto chunk = &ch->open_chunk;
			// write it then delete it
			// fill in the rest of the header
			chunk->header.op_code = 0x01;
			chunk->header.length_bytes = chunk->current_position + sizeof(chunk->header);
			chunk->header.end_time = now.usec;
			// write header
			fwrite(&chunk->header, sizeof(chunk->header), 1, f);

			// write body
			fwrite(chunk->data, 1, chunk->current_position, f);

			delete[] chunk->data;
		}

		// close the subscriber
		ps_sub_destroy(&ch->sub);
	}
	fclose(f);

	printf("Recorded %i messages\n", count);
}

struct ChannelDetails
{
	std::string topic;
	std::string type;
	ps_message_definition_t definition;
};
//okay, lets add utility functions now that the basics work
//need to be able to see the info of a bag, aka message count and list of topics
//as well as length in human readable form

//then need to be able to print out a bag, like below

// wraps around a rucksack for parsing
class Sack
{
	FILE* f_;

	rucksack::Header header_;
public:
	Sack() : f_(0)
	{

	}

	~Sack()
	{
		close();
	}

	bool create(const std::string& file)
	{

	}

	bool open(const std::string& file)
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
	char* read_block(char& out_opcode)
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

	void close()
	{
		fclose(f_);
	}

	const rucksack::Header& get_header()
	{
		return header_;
	}
};

void info(const std::string& file)
{
	// start by printing out list of topics, types and runtime
	Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	auto header = sack.get_header();

	struct ChannelInfo
	{
		std::string topic;
		std::string type;
		int count;
		uint32_t hash;
	};

	std::vector<ChannelInfo> channels;

	// now iterate through each and every chunk
	unsigned long long last_time = 0;
	char op_code;
	while (char* chunk = sack.read_block(op_code))
	{
		if (op_code == 0x02)
		{
			rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk;

			// read in the details about this topic/connection
			const char* topic = &chunk[sizeof(rucksack::ConnectionHeader)];
			const char* type = &chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1];

			ps_message_definition_t def;
			ps_deserialize_message_definition(&chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + strlen(type) + 2], &def);

			// todo handle duplicate message definitions/channels when we playback multiple files

			// insert this into our header list
			if (header->connection_id != channels.size())
			{
				printf("ERROR: Read in channel with out of order ID.");
				return;
			}

			ChannelInfo details;
			details.topic = topic;
			details.type = type;
			details.count = 0;
			details.hash = def.hash;
			channels.push_back(details);

			//printf("Got connection header\n");
		}
		else if (op_code == 0x01)
		{
			rucksack::DataChunk* header = (rucksack::DataChunk*)chunk;
			//printf("Got data chunk\n");

			if (header->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			// todo maybe should use a map?
			ChannelInfo* details = &channels[header->connection_id];

			last_time = std::max(last_time, header->end_time);

			// now can go through each message in the chunk
			int off = sizeof(rucksack::DataChunk);
			while (off < header->length_bytes)
			{
				rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&chunk[off];

				char* msg = &chunk[off + sizeof(rucksack::MessageHeader)];

				// decode yo!
				//ps_deserialize_print(msg, &details->definition);

				details->count++;

				off += hdr->length + sizeof(rucksack::MessageHeader);
			}
		}
		delete chunk;
	}

	// Now print out the information
	printf("path:       %s\n", file.c_str());
	printf("version:    %u\n", header.version);

	pubsub::Duration duration = pubsub::Time(last_time) - pubsub::Time(header.start_time);
	printf("duration:   %lfs\n", duration.toSec());
	printf("start:      %lf\n", pubsub::Time(header.start_time).toSec());
	printf("end:        %lf\n", pubsub::Time(last_time).toSec());
	//printf("size:       %d bytes\n", size);

	unsigned int total = 0;
	for (auto& details : channels)
	{
		total += details.count;
	}
	printf("messages:   %u\n", total);

	// build a list of message types
	std::map<std::string, uint32_t> types;
	for (auto& details : channels)
	{
		types[details.type] = details.hash;// todo make this a hash
	}
	printf("types:      \n");
	for (auto& type : types)
	{
		printf("  %s [%x]\n", type.first.c_str(), type.second);
	}

	printf("topics:\n");
	for (auto& details : channels)
	{
		double rate = (double)details.count / duration.toSec();
		printf("  %s    %u msgs @ %0.1lf Hz : %s\n", details.topic.c_str(), details.count, rate, details.type.c_str());
	}
}

// todo print out in order
void print(const std::string& file)
{
	Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	auto header = sack.get_header();

	// todo check version

	std::vector<ChannelDetails> channels;

	// now iterate through each and every chunk
	char op_code;
	while (char* chunk = sack.read_block(op_code))
	{
		// now lets see about the chunk
		if (op_code == 0x02)
		{
			rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk;

			// read in the details about this topic/connection
			const char* topic = &chunk[sizeof(rucksack::ConnectionHeader)];
			const char* type = &chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1];

			ps_message_definition_t def;
			ps_deserialize_message_definition(&chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + strlen(type) + 2], &def);

			// todo handle duplicate message definitions/channels when we playback multiple files

			// insert this into our header list
			if (header->connection_id != channels.size())
			{
				printf("ERROR: Read in channel with out of order ID.");
				return;
			}

			ChannelDetails details;
			details.definition = def;
			details.topic = topic;
			details.type = type;
			channels.push_back(details);
		}
		else if (op_code == 0x01)
		{
			rucksack::DataChunk* header = (rucksack::DataChunk*)chunk;

			if (header->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			// todo maybe should use a map?
			ChannelDetails* details = &channels[header->connection_id];

			// now can go through each message in the chunk
			int off = sizeof(rucksack::DataChunk);
			while (off < header->length_bytes)
			{
				rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&chunk[off];

				char* msg = &chunk[off + sizeof(rucksack::MessageHeader)];

				// decode yo!
				ps_deserialize_print(msg, &details->definition);
				printf("------------\n");

				off += hdr->length + sizeof(rucksack::MessageHeader);
			}
		}
		delete chunk;
	}
}

//then need to be able to play it back with accurate timing
//todo, sim time?
void play(const std::string& file, pubsub::ArgParser& parser)
{
	Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	auto header = sack.get_header();

	// todo check version

	ps_node_t node;
	ps_node_init(&node, "rucksack", "", true);

	struct ChannelOutput
	{
		std::shared_ptr<char[]> topic;
		std::string type;
		std::shared_ptr<ps_message_definition_t> definition;
		ps_pub_t* publisher;

		void Release()
		{
			delete publisher;
		}
	};
	std::vector<ChannelOutput> channels;

	struct ChunkInfo
	{
		const char* block;
		char* offset;
	};
	std::vector<ChunkInfo> chunks;

	// Get the start time so we know when to start making the index and accepting chunks
	const double req_start_time = parser.GetDouble("s");
	const pubsub::Time start_time = pubsub::Time(header.start_time) + pubsub::Duration(req_start_time);

	uint64_t last_time = 0;
	// now iterate through each and every chunk
	char op_code;
	while (char* chunk = sack.read_block(op_code))
	{
		// now lets see about the chunk
		if (op_code == 0x02)
		{
			rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk;

			// read in the details about this topic/connection
			const char* topic = &chunk[sizeof(rucksack::ConnectionHeader)];
			const char* type = &chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1];

			ps_message_definition_t def;
			ps_deserialize_message_definition(&chunk[sizeof(rucksack::ConnectionHeader) + strlen(topic) + strlen(type) + 2], &def);
			// give the type a name
			def.name = new char[strlen(type) + 1];
			strcpy(def.name, type);
			// todo handle duplicate message definitions/channels when we playback multiple files

			// insert this into our header list
			if (header->connection_id != channels.size())
			{
				printf("ERROR: Read in channel with out of order ID.");
				return;
			}

			// todo message definition leaks
			ChannelOutput details;
			// todo properly free the definition
			details.definition = std::make_shared<ps_message_definition_t>(def);			
			details.topic = std::shared_ptr<char[]>(new char[strlen(topic) + 1]);
			strcpy(details.topic.get(), topic);
			details.type = type;
			details.publisher = new ps_pub_t;
			// create the publisher
			// todo handle latched
			ps_node_create_publisher(&node, details.topic.get(), details.definition.get(), details.publisher, false);
			channels.push_back(details);

			//printf("Got connection header\n");

			delete[] chunk;
		}
		else if (op_code == 0x01)
		{
			rucksack::DataChunk* header = (rucksack::DataChunk*)chunk;
			//printf("Got data chunk\n");

			if (header->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			// if the chunk is too new, ignore it
			if (header->start_time < start_time.usec)
			{
				delete[] chunk;
				continue;
			}

			last_time = std::max(header->end_time, last_time);

			chunks.push_back({ chunk, chunk + sizeof(rucksack::DataChunk) });
		}
		else
		{
			delete[] chunk;
		}
	}

	printf("Warning: this bag is unordered so playback may take a moment to begin..\n");

	// okay, lets build an index for playing this back...
	// sort chunks by start time  (should already be in order honestly...)
	std::sort(chunks.begin(), chunks.end(),
		[](const ChunkInfo& a, const ChunkInfo& b) -> bool
	{
		const rucksack::DataChunk* header_a = (const rucksack::DataChunk*)a.block;
		const rucksack::DataChunk* header_b = (const rucksack::DataChunk*)b.block;
		return header_a->start_time < header_b->start_time;
	});

	// now build index by looping over chunks
	struct MessageIndex
	{
		uint64_t time;
		ChannelOutput* channel;
		const char* ptr;
	};
	std::vector<MessageIndex> index;
	index.reserve(1000000);// should be good enough for anyone
	for (int i = 0; i < chunks.size(); i++)
	{
		const char* chunk = chunks[i].block;

		const rucksack::DataChunk* header = (rucksack::DataChunk*)chunk;

		// todo maybe should use a map?
		ChannelOutput* details = &channels[header->connection_id];

		// now can go through each message in the chunk
		int off = sizeof(rucksack::DataChunk);
		while (off < header->length_bytes)
		{
			const rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&chunk[off];
			const char* data = &chunk[off + sizeof(rucksack::MessageHeader)];

			if (ps_okay() == false)
			{
				return;
			}

			//ps_node_spin(&node);

			MessageIndex idx;
			idx.ptr = &chunk[off];
			idx.time = hdr->time;
			idx.channel = details;


			off += hdr->length + sizeof(rucksack::MessageHeader);

			if (idx.time < start_time.usec)
			{
				continue;
			}
			index.push_back(idx);
		}
	}
	
	// I guess lets just sort by time?
	std::sort(index.begin(), index.end(),
		[](const MessageIndex& a, const MessageIndex& b) -> bool
	{
		return a.time < b.time;
	});

	printf("Done sorting...\n");

	// give the node a bit to advertise
	wait(&node);

	
	const double length = (pubsub::Time(last_time) - start_time).toSec();

	const double time_scale = parser.GetDouble("r");
	const double inv_time_scale = 1.0 / time_scale;
	const double inv_time_scale_ms = inv_time_scale / 1000.0;

	pubsub::Time real_begin = pubsub::Time::now();

	// now lets start publishing each chunk
	for (int i = 0; i < index.size(); i++)
	{
		rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)index[i].ptr;
		const char* data = index[i].ptr + sizeof(rucksack::MessageHeader);

		if (ps_okay() == false)
		{
			return;
		}

		// wait until the next message
		double diff = (hdr->time - start_time.usec);
		double real_diff;
		while (real_diff = ((pubsub::Time::now().usec - real_begin.usec)*time_scale), (diff > real_diff))// converts to "sim time"
		{
			double dt = (diff - real_diff)*inv_time_scale_ms;
			ps_sleep(std::max<int>(dt, 0));
		}

		//ps_deserialize_print(data, details->definition.get());

		// for the moment, lets just publish everything immediately
		ps_msg_t msg;
		ps_msg_alloc(hdr->length, &msg);
		memcpy(ps_get_msg_start(msg.data), data, hdr->length);
		ps_pub_publish(index[i].channel->publisher, &msg);

		ps_node_spin(&node);
			
		// todo print less often
		printf("\r [RUNNING]  Sack Time: %13.6f   Duration: %.6f / %.6f               \r", 
		       pubsub::Time(hdr->time).toSec(),
			   (pubsub::Time(hdr->time) - start_time).toSec(),
			   length);
	}

	for (int i = 0; i < chunks.size(); i++)
	{
		delete[] chunks[i].block;
	}

	ps_node_destroy(&node);
}


int main(int argc, char** argv)
{
	std::string verb = argc > 1 ? argv[1] : "";

	//play("C:/Users/space/Desktop/pubsub_proto/build/x86-Debug/rucksack/test.sack");
	//return 0;
	if (verb == "record")
	{
		pubsub::ArgParser parser;
		parser.AddMulti({ "a", "all" }, "Record all topics", "true");

		parser.Parse(argv, argc, 1);

		//record("C:/Users/space/Desktop/pubsub_proto/test.sack");

		record(parser.GetPositional(0));
	}
	else if (verb == "info")
	{
		pubsub::ArgParser parser;
		//parser.AddMulti({ "a", "all" }, "Record all topics", "true");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		for (auto file : files)
		{
			info(file);
		}
	}
	else if (verb == "play")
	{
		pubsub::ArgParser parser;
		//parser.AddMulti({ "a", "all" }, "Record all topics", "true");
		parser.AddMulti({ "r" }, "Rate to play the bag at.", "1.0");
		parser.AddMulti({ "s" }, "Offset to start playing the bag at.", "0.0");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		for (auto file : files)
		{
			play(file, parser);
		}
		//play("C:/Users/space/Desktop/pubsub_proto/test.sack");
	}
	else if (verb == "print")
	{
		pubsub::ArgParser parser;
		//parser.AddMulti({ "a", "all" }, "Record all topics", "true");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		for (auto file : files)
		{
			print(file);
		}
		//play("C:/Users/space/Desktop/pubsub_proto/test.sack");
	}
	return 0;
}