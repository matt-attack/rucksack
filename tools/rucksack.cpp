
#include <pubsub/TCPTransport.h>

#include <pubsub/Node.h>
#include <pubsub/Subscriber.h>
#include <pubsub/Publisher.h>

#include <rucksack/datastructures.h>
#include <rucksack/rucksack.h>

#include <pubsub_cpp/arg_parse.h>
#include <pubsub_cpp/Time.h>

#include <pubsub/System.h>

#include <cmath>
#include <cstring>
#include <mutex>
#include <algorithm>
#include <thread>
#include <memory>

#ifdef _WIN32
#include <conio.h>
#else
#include <stdio.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <termios.h>
//#include <stropts.h>

int _kbhit() {
	static const int STDIN = 0;
	static bool initialized = false;

	if (!initialized) {
		// Use termios to turn off line buffering
		termios term;
		tcgetattr(STDIN, &term);
		term.c_lflag &= ~ICANON;
		tcsetattr(STDIN, TCSANOW, &term);
		setbuf(stdin, NULL);
		initialized = true;
	}

	int bytesWaiting;
	ioctl(STDIN, FIONREAD, &bytesWaiting);
	return bytesWaiting;
}
#endif

#undef max

void wait(ps_node_t* node)
{
	printf("Waiting for connections...\n\n");
	uint64_t start = GetTimeMs();
	while (ps_okay() && start + 3000 > GetTimeMs())
	{
		ps_node_wait(node, 100);
		ps_node_spin(node);
	}
	printf("Done waiting for connections...\n\n");
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
};
std::map<std::string, TopicData> _topics;

struct Channel
{
	QueueChunk open_chunk;
	ps_sub_t sub;
	uint32_t id;

	TopicData topic;

	int chunk_size;

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

inline void HandleMessage(Channel* channel, void* message, unsigned int size, pubsub::Time now)
{
	// get and deserialize the messages

		// check if we saved the channel, if not do that
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

	const int chunk_size = channel->chunk_size;// todo change this to be a parameter and make it bigger
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
	memcpy(chunk.data + chunk.current_position, &header, sizeof(header));
	chunk.current_position += sizeof(header);

	// then write the data
	memcpy(chunk.data + chunk.current_position, message, size);
	chunk.current_position += size;

	free(message);
}

std::vector<std::string> split(std::string s, std::string delimiter) {
	size_t pos_start = 0, pos_end, delim_len = delimiter.length();
	std::string token;
	std::vector<std::string> res;

	while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
		token = s.substr(pos_start, pos_end - pos_start);
		pos_start = pos_end + delim_len;
		res.push_back(token);
	}

	res.push_back(s.substr(pos_start));
	return res;
}

void record(pubsub::ArgParser& parser)
{
	std::string filename = std::to_string(std::round(pubsub::Time::now().toSec())) + ".sack";
	if (parser.GetString("o").length() > 0)
	{
		filename = parser.GetString("o");
	}

	printf("Recording to '%s'\n", filename.c_str());

	// open the file
	FILE* f = fopen(filename.c_str(), "wb");

	// read in parameters
	static unsigned int max_count = std::numeric_limits<unsigned int>::max();
	if (parser.GetDouble("n") > 0)
	{
		max_count = parser.GetDouble("n");
	}
	pubsub::Duration max_length = pubsub::Duration(std::numeric_limits<int64_t>::max());
	if (parser.GetDouble("d") > 0)
	{
		max_length = pubsub::Duration(parser.GetDouble("d"));
	}
	const int chunk_size = parser.GetDouble("c") * 1000;

	// first lets get a list of all topics, and then subscribe to them
	ps_node_t node;
	ps_node_init(&node, "rucksack", "", true);

	struct ps_transport_t tcp_transport;
	ps_tcp_transport_init(&tcp_transport, &node);
	ps_node_add_transport(&node, &tcp_transport);

	ps_node_system_query(&node);

	node.adv_cb = [](const char* topic, const char* type, const char* node, const ps_advertise_req_t* data)
	{
		// todo check if we already have the topic
		if (_topics.find(topic) != _topics.end())
		{
			return;
		}

		printf("Found topic %s..\n", topic);

		TopicData tdata;
		tdata.topic = topic;
		tdata.type = type;
		tdata.hash = data->type_hash;
		_topics[topic] = tdata;
	};

	static bool recording = true;
	// create the data saving thread
	std::thread saver([f]()
		{
			while (ps_okay() && recording)
			{
				// check for channels and save em if we have em
				std::vector<QueueChannel> ch_to_save;
				_channel_mutex.lock();
				std::swap(ch_to_save, _finished_channels);
				_channel_mutex.unlock();

				// now write each of the connection headers to the file
				for (auto& chunk : ch_to_save)
				{
					//printf("Saving channel for topic\n");
					rucksack::ConnectionHeader header;
					header.header.op_code = rucksack::constants::ConnectionHeaderOp;
					header.connection_id = chunk.channel->id;
					header.hash = chunk.channel->topic.hash;

					char buf[1500];
					int def_len = ps_serialize_message_definition(buf, &chunk.channel->sub.received_message_def);

					int data_length = def_len + chunk.channel->topic.topic.length() + 1;
					header.header.length_bytes = data_length + sizeof(header);
					fwrite(&header, sizeof(header), 1, f);

					// then goes the topic name string
					fwrite(chunk.channel->topic.topic.c_str(), 1, chunk.channel->topic.topic.length() + 1, f);

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
					chunk.header.header.op_code = rucksack::constants::DataChunkOp;
					chunk.header.header.length_bytes = chunk.current_position + sizeof(chunk.header);

					// write header
					fwrite(&chunk.header, sizeof(chunk.header), 1, f);

					// write body
					fwrite(chunk.data, 1, chunk.current_position, f);

					delete[] chunk.data;
				}

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

	auto raw_topics_to_record = parser.GetAllPositional();

	struct TopicInfo
	{
		std::string topic;
		int skip_factor;
	};
	std::vector<TopicInfo> topics_to_record;
	for (auto& topic : raw_topics_to_record)
	{
		auto splits = split(topic, ":");
		TopicInfo ti;
		ti.topic = splits[0];
		if (splits.size() > 1)
		{
			ti.skip_factor = std::atoi(splits[1].c_str());
		}
		else
		{
			ti.skip_factor = 0;
		}
		topics_to_record.push_back(ti);
	}

	struct TodoMsg
	{
		void* data;
		unsigned int length;
		Channel* channel;
		pubsub::Time time;
	};
	static std::vector<TodoMsg> todo_msgs;
	node.def_cb = [](const struct ps_message_definition_t* definition)
	{
		for (int i = 0; i < todo_msgs.size(); i++)
		{
			auto& msg = todo_msgs[i];
			if (msg.channel->sub.received_message_def.fields != 0)
			{
				// good to process
				HandleMessage(msg.channel, msg.data, msg.length, msg.time);

				todo_msgs.erase(todo_msgs.begin() + i);
				i--;
			}
		}
	};

	static int count = 0;
	// now subscribe to things!
	node.adv_cb = 0;// todo dont do this
	for (auto& topic : _topics)
	{
		// see if we found it
		bool found = false;
		int skip_factor = 0;
		for (auto& t : topics_to_record)
		{
			if (topic.first == t.topic)
			{
				found = true;
				skip_factor = t.skip_factor;
				break;
			}
		}
		if (!found)
		{
			continue;
		}

		printf("Subscribing to %s\n", topic.first.c_str());
		struct ps_subscriber_options options;
		ps_subscriber_options_init(&options);
		options.preferred_transport = 1;// prefer tcp
		options.skip = skip_factor;
		options.queue_size = 0;
		options.want_message_def = true;
		options.allocator = 0;
		options.ignore_local = false;
		options.cb = [](void* message, unsigned int size, void* data, const ps_msg_info_t* info)
		{
			if (count >= max_count)
			{
				return;
			}

			count++;
			//todo need to make sure we dont wildcard subscribe on the topic without checking that they are all the same time
			Channel* channel = (Channel*)data;

			pubsub::Time now = pubsub::Time::now();
			if (channel->sub.received_message_def.fields == 0)
			{
				// queue it up, then print them out once I get it
				TodoMsg msg;
				msg.data = message;
				msg.length = size;
				msg.channel = channel;
				msg.time = now;
				todo_msgs.push_back(msg);
				return;
			}

			HandleMessage(channel, message, size, now);
		};

		Channel* new_channel = new Channel;
		new_channel->id = _channel_id++;
		new_channel->topic = topic.second;
		new_channel->chunk_size = chunk_size;

		options.cb_data = new_channel;
		ps_node_create_subscriber_adv(&node, topic.first.c_str(), 0, &new_channel->sub, &options);

		_channels.push_back(new_channel);
	}

	// spin away!
	while (ps_okay())
	{
		// check if we should stop
		if (count >= max_count || (pubsub::Time::now() - header.start_time) > max_length)
		{
			break;
		}
		// todo blocking
		ps_node_spin(&node);
		ps_sleep(1);
	}

	recording = false;
	saver.join();

	// now save out any open chunks and close channels
	pubsub::Time now = pubsub::Time::now();
	for (auto& ch : _channels)
	{
		if (ch->open_chunk.data && ch->open_chunk.current_position > 0)
		{
			// todo use last message time for end time
			auto chunk = &ch->open_chunk;
			// write it then delete it
			// fill in the rest of the header
			chunk->header.header.op_code = rucksack::constants::DataChunkOp;
			chunk->header.header.length_bytes = chunk->current_position + sizeof(chunk->header);

			// determine real end time
			// loop through the messages and find the last one
			unsigned int pos = 0;
			uint64_t time = now.usec;
			while (pos < chunk->current_position)
			{
				const rucksack::MessageHeader* header = (rucksack::MessageHeader*)&chunk->data[pos];
				chunk->header.end_time = header->time;
				pos += header->length + sizeof(rucksack::MessageHeader);
			}

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

	// warn about any messages we never actually saved because we didnt get a message definition
	if (todo_msgs.size())
	{
		printf("WARNING: Dropped %zi messages because we couldn't find their message definition.\n", todo_msgs.size());
	}
}

struct ChannelDetails
{
	std::string topic;
	std::string type;
	ps_message_definition_t definition;
};
//okay, lets add utility functions now that the basics work
//need to be able to see the info of a rucksack, aka message count and list of topics
//as well as length in human readable form

//then need to be able to print out a rucksack, like below

void info(const std::string& file, pubsub::ArgParser& parser)
{
	// start by printing out list of topics, types and runtime
	rucksack::Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	bool verbose = parser.GetBool("v");

	auto header = sack.get_header();

	struct ChannelInfo
	{
		std::string topic;
		std::string type;
		int count;
		uint32_t hash;
		ps_message_definition_t definition;
	};

	std::vector<ChannelInfo> channels;

	// now iterate through each and every chunk
	uint64_t last_time = 0;
	unsigned int n_chunks = 0;
	char op_code;
	while (char* chunk_ptr = sack.read_block(op_code))
	{
		if (op_code == rucksack::constants::ConnectionHeaderOp)
		{
			rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk_ptr;

			// read in the details about this topic/connection
			const char* topic = &chunk_ptr[sizeof(rucksack::ConnectionHeader)];

			ChannelInfo details;
			ps_deserialize_message_definition(&chunk_ptr[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1],
				&details.definition);

			// todo handle duplicate message definitions/channels when we playback multiple files

			// insert this into our header list
			if (header->connection_id >= channels.size())
			{
				channels.resize(header->connection_id + 1);
			}

			details.topic = topic;
			details.type = details.definition.name;
			details.count = 0;
			details.hash = details.definition.hash;
			channels[header->connection_id] = details;
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			rucksack::DataChunk* chunk = (rucksack::DataChunk*)chunk_ptr;
			n_chunks++;
			//printf("Got data chunk\n");

			if (chunk->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			// todo maybe should use a map?
			ChannelInfo* details = &channels[chunk->connection_id];

			last_time = std::max(last_time, chunk->end_time);

			// now can go through each message in the chunk
			int off = sizeof(rucksack::DataChunk);
			while (off < chunk->header.length_bytes)
			{
				rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&chunk_ptr[off];

				details->count++;

				off += hdr->length + sizeof(rucksack::MessageHeader);
			}
		}
		delete[] chunk_ptr;
	}

	// Quickly grab file size
	uint64_t size = 0;
	FILE* f = fopen(file.c_str(), "rb");
	if (f)
	{
		fseek(f, 0L, SEEK_END);
		size = ftell(f);
		fclose(f);
	}

	// Now print out the information
	printf("path:       %s\n", file.c_str());
	printf("version:    %u\n", header.version);

	pubsub::Duration duration = pubsub::Time(last_time) - pubsub::Time(header.start_time);
	printf("duration:   %lfs\n", duration.toSec());
	printf("start:      %s\n", pubsub::Time(header.start_time).toString().c_str());
	printf("end:        %s\n", pubsub::Time(last_time).toString().c_str());
	printf("chunks:     %u\n", n_chunks);

	unsigned int total = 0;
	for (auto& details : channels)
	{
		total += details.count;
	}
	printf("messages:   %u\n", total);

	if (size > 1000 * 1000 * 1000)
	{
		printf("size:       %0.3f GB\n", size / 1000000000.0);
	}
	else if (size > 1000 * 1000)
	{
		printf("size:       %0.3f MB\n", size / 1000000.0);
	}
	else if (size > 1000)
	{
		printf("size:       %0.3f KB\n", size / 1000.0);
	}
	else
	{
		printf("size:       %lu bytes\n", size);
	}

	// build a list of message types (sort and deduplicate)
	std::map<std::string, ChannelInfo> types;
	for (auto& details : channels)
	{
		types[details.type] = details;
	}
	printf("types:      \n");
	for (auto& type : types)
	{
		printf("  %s [%x]\n", type.first.c_str(), type.second.hash);
		if (verbose)
		{
			// print out the message definition
			printf("-----------------\n");
			ps_print_definition(&type.second.definition, false);
			printf("-----------------\n");
		}
	}

	printf("topics:\n");
	for (auto& details : channels)
	{
		double rate = (double)details.count / duration.toSec();
		printf("  %s    %u msgs @ %0.1lf Hz : %s\n", details.topic.c_str(), details.count, rate, details.type.c_str());
	}

	// Free the channel infos
	for (auto& info : channels)
	{
		ps_free_message_definition(&info.definition);
	}
}

// todo print out in order
void print(const std::string& file, pubsub::ArgParser& parser)
{
	rucksack::SackReader sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	bool verbose = parser.GetBool("v");

	rucksack::MessageHeader const* hdr;
	rucksack::SackChannelDetails const* def;
	while (const void* msg = sack.read(hdr, def))
	{
		if (verbose)
		{
			printf("timestamp: %" PRIu64 "\n", hdr->time);
		}
		ps_deserialize_print(msg, &def->definition, 0, 0);
		printf("------------\n");
	}

	return;
}

//then need to be able to play it back with accurate timing
//todo, sim time?

void play(const std::vector<std::string>& files, pubsub::ArgParser& parser)
{
	std::string file = files[0];
	rucksack::Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	auto header = sack.get_header();

	// todo check version

	ps_node_t node;
	ps_node_init(&node, "rucksack", "", true);

	struct ps_transport_t tcp_transport;
	ps_tcp_transport_init(&tcp_transport, &node);
	ps_node_add_transport(&node, &tcp_transport);

	struct ChannelOutput
	{
		std::shared_ptr<char[]> topic;
		std::string type;
		std::shared_ptr<ps_message_definition_t> definition;
		ps_pub_t* publisher;

		void Release()
		{
			ps_free_message_definition(definition.get());
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

	const bool loop = parser.GetBool("l");

	uint64_t last_time = 0;
	// now iterate through each and every chunk
	char op_code;
	while (char* chunk_ptr = sack.read_block(op_code))
	{
		// now lets see about the chunk
		if (op_code == rucksack::constants::ConnectionHeaderOp)
		{
			rucksack::ConnectionHeader* header = (rucksack::ConnectionHeader*)chunk_ptr;

			// read in the details about this topic/connection
			const char* topic = &chunk_ptr[sizeof(rucksack::ConnectionHeader)];

			ps_message_definition_t def;
			ps_deserialize_message_definition(&chunk_ptr[sizeof(rucksack::ConnectionHeader) + strlen(topic) + 1], &def);

			// todo handle duplicate message definitions/channels when we playback multiple files

			// insert this into our header list
			if (header->connection_id >= channels.size())
			{
				channels.resize(header->connection_id + 1);
			}

			// todo message definition leaks
			ChannelOutput details;
			// todo properly free the definition
			details.definition = std::make_shared<ps_message_definition_t>(def);
			details.topic = std::shared_ptr<char[]>(new char[strlen(topic) + 1]);
			strcpy(details.topic.get(), topic);
			details.type = def.name;
			details.publisher = new ps_pub_t;
			// create the publisher
			// todo handle latched
			ps_node_create_publisher(&node, details.topic.get(), details.definition.get(), details.publisher, true);
			channels[header->connection_id] = details;

			//printf("Got connection header\n");

			delete[] chunk_ptr;
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			rucksack::DataChunk* chunk = (rucksack::DataChunk*)chunk_ptr;
			//printf("Got data chunk\n");

			if (chunk->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			// if the chunk is too new, ignore it
			if (chunk->start_time < start_time.usec)
			{
				delete[] chunk_ptr;
				continue;
			}

			last_time = std::max(chunk->end_time, last_time);

			chunks.push_back({ chunk_ptr, chunk_ptr + sizeof(rucksack::DataChunk) });
		}
		else
		{
			delete[] chunk_ptr;
		}
	}

	printf("Warning: this rucksack is unordered so playback may take a moment to begin..\n");

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
		const char* chunk_ptr = chunks[i].block;

		const rucksack::DataChunk* chunk = (rucksack::DataChunk*)chunk_ptr;

		// todo maybe should use a map?
		ChannelOutput* details = &channels[chunk->connection_id];

		// now can go through each message in the chunk
		int off = sizeof(rucksack::DataChunk);
		while (off < chunk->header.length_bytes)
		{
			const rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)&chunk_ptr[off];
			const char* data = &chunk_ptr[off + sizeof(rucksack::MessageHeader)];

			if (ps_okay() == false)
			{
				return;
			}

			MessageIndex idx;
			idx.ptr = &chunk_ptr[off];
			idx.time = hdr->time;
			idx.channel = details;


			off += hdr->length + sizeof(rucksack::MessageHeader);

			// Ignore this message if it's before our start time
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
		while (real_diff = ((pubsub::Time::now().usec - real_begin.usec) * time_scale), (diff > real_diff))// converts to "sim time"
		{
			double dt = (diff - real_diff) * inv_time_scale_ms;
			ps_sleep(std::max<int>(dt, 0));
		}

		ps_msg_t msg;
		ps_msg_alloc(hdr->length, 0, &msg);
		memcpy(ps_get_msg_start(msg.data), data, hdr->length);
		ps_pub_publish(index[i].channel->publisher, &msg);

		// todo spin less often
		ps_node_spin(&node);

		// todo print less often
		printf("\r [PLAYING]  Sack Time: %13.6f   Duration: %.6f / %.6f               \r",
			pubsub::Time(hdr->time).toSec(),
			(pubsub::Time(hdr->time) - start_time).toSec(),
			length);
		fflush(stdout);

		// handle pausing
		if (_kbhit() && getc(stdin) == ' ')
		{
			while (true)
			{
				// handle paused
				printf("\r [PAUSED]  Sack Time: %13.6f   Duration: %.6f / %.6f               \r",
					pubsub::Time(hdr->time).toSec(),
					(pubsub::Time(hdr->time) - start_time).toSec(),
					length);

				ps_node_spin(&node);
				ps_sleep(1);

				if ((_kbhit() && getc(stdin) == ' ') || ps_okay() == false)
				{
					break;
				}
			}
		}

		// if in loop mode, go back to beginning at the end
		if (loop && i == index.size() - 1)
		{
			i = 0;
			real_begin = pubsub::Time::now();
		}
	}

	for (int i = 0; i < chunks.size(); i++)
	{
		delete[] chunks[i].block;
	}

	ps_node_destroy(&node);

	// Free the channel infos
	for (auto& info : channels)
	{
		info.Release();
	}
}

void merge(std::vector<std::string> files, pubsub::ArgParser& parser)
{
	// Create the file
	rucksack::SackWriter osack;

	std::string out_file = parser.GetString("o");
	if (out_file.length() == 0)
	{
		printf("ERROR: Must provide output file name.\n");
		return;
	}

	// load each file and get the lowest start time
	pubsub::Time start_time = pubsub::Time(std::numeric_limits<uint64_t>::max());
	std::vector<rucksack::SackReader*> sacks;
	for (auto& file: files)
	{
		auto sack = new rucksack::SackReader();
		if (!sack->open(file))
		{
			printf("ERROR: Opening sack '%s' failed!\n", file.c_str());
			return;
		}
		sacks.push_back(sack);

		auto header = sack->get_header();
		if (start_time > pubsub::Time(header.start_time))
		{
			start_time = pubsub::Time(header.start_time);
		}
	}

	// todo need chunk size
	osack.create(out_file, start_time, 10000);

	for (auto& sack: sacks)
	{
		rucksack::MessageHeader const* hdr;
		rucksack::SackChannelDetails const* info;
		while (const void* msg = sack->read(hdr, info))
		{
			if (!osack.write_message(info->topic, msg, hdr->length, &info->definition, pubsub::Time(hdr->time)))
			{
				printf("ERROR: Messages on topic '%s' had mismatched definitions.\n", info->topic.c_str());
				return;
			}
		}
	}

	for (auto& file: sacks)
	{
		delete file;
	}

	osack.close();
}

void print_help()
{
	printf("Usage: rucksack <verb> (arg1) (arg2) ...\n"
		" Verbs:\n"
		"   info (file names)\n"
		"   play (file names)\n"
		"   print (file names)\n"
		"   record (topic name)\n");
}


int main(int argc, char** argv)
{
	std::string verb = argc > 1 ? argv[1] : "";

	pubsub::ArgParser parser;

	if (verb == "record")
	{
		parser.SetUsage("Usage: rucksack record TOPIC... [OPTION...]\n\nRecords topics to a file.\n");
		parser.AddMulti({ "a", "all" }, "Record all topics", "true");
		parser.AddMulti({ "c", "chunk-size" }, "Chunk size in kB", "16");// todo increase me later
		parser.AddMulti({ "d", "duration" }, "Length in seconds to record", "-1.0");
		parser.AddMulti({ "n" }, "Number of messages to record", "-1");
		parser.AddMulti({ "o" }, "Name of sack file", "");

		parser.Parse(argv, argc, 1);

		record(parser);
	}
	else if (verb == "info")
	{
		parser.SetUsage("Usage: rucksack info FILE... [OPTION...]\n\nPrints information about rucksack files.\n");
		parser.AddMulti({ "v" }, "Print additional info", "false");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		for (auto file : files)
		{
			info(file, parser);
		}
	}
	else if (verb == "play")
	{
		parser.SetUsage("Usage: rucksack play FILE... [OPTION...]\n\nPlays back topics stored in rucksack files.\n");
		parser.AddMulti({ "r" }, "Rate to play the rucksack at.", "1.0");
		parser.AddMulti({ "s" }, "Offset to start playing the rucksack at.", "0.0");
		parser.AddMulti({ "l" }, "If set, loop through the rucksack rather than stopping at the end.");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		play(files, parser);
	}
	else if (verb == "print")
	{
		parser.SetUsage("Usage: rucksack print FILE...\n\nPrints each message stored in rucksack files.");
		parser.AddMulti({ "v" }, "Print timestamps along with each message.", "false");
		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		for (auto file : files)
		{
			print(file, parser);
		}
	}
	else if (verb == "merge")
	{
		// merges two sack files into one
		parser.SetUsage("Usage: rucksack merge FILE... -o OUTPUT_FILE\n\nCombines the contents of multiple sack files into one.");
		parser.AddMulti({ "o", "output" }, "Name for output combined sack file.", "");
		parser.Parse(argv, argc, 1);

		merge(parser.GetAllPositional(), parser);
	}
	else
	{
		// give top level help
		print_help();
	}
	return 0;
}
