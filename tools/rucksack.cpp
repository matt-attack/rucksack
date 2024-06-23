
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
	FILE* file;
};

std::mutex _chunk_mutex;
std::vector<QueueChunk> _finished_chunks;

struct Channel;
struct QueueChannel
{
	Channel* channel;
	FILE* file;
};
std::mutex _channel_mutex;
std::vector<QueueChannel> _finished_channels;

// a list of topics we end up wanting to subscribe to

struct TopicData
{
	std::string topic;
	std::string type;
	uint32_t hash;
	bool latched;
};
std::map<std::string, TopicData> _topics;

struct Channel
{
	QueueChunk open_chunk;
	pubsub::Time open_chunk_last_message_time;
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

    void Reset()
    {
        written = false;
        open_chunk.data = 0;
        open_chunk.current_position = 0;
    }
};
std::vector<Channel*> _channels;
uint32_t _channel_id = 0;
FILE* _current_sack_file = 0;
double _current_sack_size = 0;

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

	channel->open_chunk_last_message_time.usec = now.usec;

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
	std::string filename = std::to_string((int)std::round(pubsub::Time::now().toSec())) + ".sack";
	if (parser.GetString("o").length() > 0)
	{
		filename = parser.GetString("o");
	}

	bool record_all_topics = parser.GetBool("a");
	auto raw_topics_to_record = parser.GetAllPositional();

	if (raw_topics_to_record.size() == 0 && !record_all_topics)
	{
		printf("record: must list topics to record or use -a\n");
		return;
	}

	double split_size = parser.GetDouble("ss");
	double split_duration = parser.GetDouble("sd");

	if (split_size <= 0.0)
	{
		split_size = std::numeric_limits<double>::max();
	}

	auto ps_spit_duration = pubsub::Duration(std::numeric_limits<int64_t>::max());
	if (split_duration > 0.0)
	{
		ps_spit_duration = pubsub::Duration(split_duration);
	}

	printf("Recording to '%s'\n", filename.c_str());

	// open the file
	_current_sack_file = fopen(filename.c_str(), "wb");
	std::vector<FILE*> files;
	files.push_back(_current_sack_file);

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

	node.adv_cb = [](const char* topic, const char* type, const char* node, const ps_advertise_req_t* data, void* cb_data)
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
		tdata.latched = ((data->flags & PS_ADVERTISE_LATCHED) != 0) ? true : false;
		_topics[topic] = tdata;
	};

	static bool recording = true;
	// create the data saving thread
	std::thread saver([]()
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
					header.flags = chunk.channel->topic.latched ? rucksack::constants::CHFLAG_LATCHED : 0;

					char buf[1500];
					int def_len = ps_serialize_message_definition(buf, &chunk.channel->sub.received_message_def);

					int data_length = def_len + chunk.channel->topic.topic.length() + 1;
					header.header.length_bytes = data_length + sizeof(header);
					fwrite(&header, sizeof(header), 1, _current_sack_file);

					// then goes the topic name string
					fwrite(chunk.channel->topic.topic.c_str(), 1, chunk.channel->topic.topic.length() + 1, _current_sack_file);

					// then the message definition
					fwrite(buf, def_len, 1, _current_sack_file);
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
					fwrite(&chunk.header, sizeof(chunk.header), 1, _current_sack_file);

					// write body
					fwrite(chunk.data, 1, chunk.current_position, _current_sack_file);

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
	auto start_time = pubsub::Time::now();
	rucksack::Header header;
	header.magic_number = rucksack::constants::MagicNumber;
	header.start_time = start_time.usec;
	header.version = rucksack::constants::CurrentVersion;
	fwrite(&header, sizeof(header), 1, _current_sack_file);

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
	node.def_cb = [](const struct ps_message_definition_t* definition, void* cb_data)
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
		bool record = record_all_topics;
		int skip_factor = 0;
		for (auto& t : topics_to_record)
		{
			if (topic.first == t.topic)
			{
				record = true;
				skip_factor = t.skip_factor;
				break;
			}
		}
		if (!record)
		{
			continue;
		}

		printf("Subscribing to %s\n", topic.first.c_str());
		struct ps_subscriber_options options;
		ps_subscriber_options_init(&options);
		options.preferred_transport = 1;// prefer tcp
		options.skip = skip_factor;
		options.queue_size = 0;
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

	// Spin away!
	int bag_number = 1;
	while (ps_okay())
	{
		auto now = pubsub::Time::now();
		pubsub::Duration bag_duration = now - header.start_time;
		pubsub::Duration total_duration = now - start_time;

		// Check if we should stop
		if (count >= max_count || total_duration > max_length)
		{
			break;
		}

		// Check if we should split for going over time or size
		if (bag_duration > ps_spit_duration || _current_sack_size > split_size)
		{
			// Save any open chunks and swap to a new file
			for (auto& ch : _channels)
			{
				auto& chunk = ch->open_chunk;
				if (chunk.data && chunk.current_position > 0)
				{
					chunk.header.end_time = ch->open_chunk_last_message_time.usec;
					chunk.file = _current_sack_file;
					_chunk_mutex.lock();
					_finished_chunks.push_back(chunk);
					_chunk_mutex.unlock();
				}

				// Reset it for use with the new bag
				ch->Reset();
			}

			// Now open a new file to record to
			std::string bag_name = filename + std::to_string(bag_number);
			bag_number++;

			printf("Recording to new bag '%s'\n", bag_name.c_str());
			_current_sack_file = fopen(bag_name.c_str(), "wb");
			_current_sack_size = 0;

			// Write the header, this might block a bit in this thread, but meh its rare
			//rucksack::Header header;
			header.magic_number = rucksack::constants::MagicNumber;
			header.start_time = pubsub::Time::now().usec;
			header.version = rucksack::constants::CurrentVersion;
			fwrite(&header, sizeof(header), 1, _current_sack_file);

			files.push_back(_current_sack_file);
		}
		ps_node_wait(&node, 1);
		ps_node_spin(&node);
	}

	recording = false;
	saver.join();

	// now save out any open chunks and close channels
	pubsub::Time now = pubsub::Time::now();
	for (auto& ch : _channels)
	{
		// save the chunk if open
		if (ch->open_chunk.data && ch->open_chunk.current_position > 0)
		{
			auto chunk = &ch->open_chunk;
			// write it then delete it
			// fill in the rest of the header
			chunk->header.header.op_code = rucksack::constants::DataChunkOp;
			chunk->header.header.length_bytes = chunk->current_position + sizeof(chunk->header);
            chunk->header.end_time = ch->open_chunk_last_message_time.usec;

			// write header
			fwrite(&chunk->header, sizeof(chunk->header), 1, _current_sack_file);

			// write body
			fwrite(chunk->data, 1, chunk->current_position, _current_sack_file);

			delete[] chunk->data;
		}

		// close the subscriber
		ps_sub_destroy(&ch->sub);
	}

	// Close all the files we opened
	for (auto& file: files)
	{
		fclose(file);
	}

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

void info(const std::string& file, pubsub::ArgParser& parser)
{
	// start by printing out list of topics, types and runtime
	rucksack::Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack '%s' failed!\n", file.c_str());
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
		bool latched;
		ps_message_definition_t definition;
	};

	std::vector<ChannelInfo> channels;
	// now iterate through each and every chunk
	uint64_t last_time = 0;
	uint64_t first_time = std::numeric_limits<uint64_t>::max();
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
			details.latched = (header->flags & rucksack::constants::CHFLAG_LATCHED) != 0;
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

			first_time = std::min(first_time, chunk->start_time);
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

	unsigned int total = 0;
	for (auto& details: channels)
	{
		total += details.count;
	}

	// Now print out the information
	printf("path:       %s\n", file.c_str());
	printf("version:    %u\n", header.version);
	printf("timestamp:  %s\n", pubsub::Time(header.start_time).toString().c_str());

	pubsub::Duration duration = pubsub::Time(last_time) - pubsub::Time(first_time);
	if (total != 0)
	{
		printf("duration:   %lfs\n", duration.toSec());
		printf("start:      %s\n", pubsub::Time(first_time).toString().c_str());
		printf("end:        %s\n", pubsub::Time(last_time).toString().c_str());
	}
	else
	{
		printf("duration:   0s\n");
		printf("start:      N/A\n");
		printf("end:        N/A\n");
	}
	printf("chunks:     %u\n", n_chunks);

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
	for (auto& details: channels)
	{
		types[details.type] = details;
	}
	printf("types:      \n");
	for (auto& type: types)
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
	for (auto& details: channels)
	{
		double rate = (double)details.count / duration.toSec();
		printf("  %s    %u msgs @ %0.1lf Hz : %s%s\n", details.topic.c_str(), details.count, rate, details.type.c_str(), details.latched ? " (latched)" : "");
	}

	// Free the channel infos
	for (auto& info : channels)
	{
		ps_free_message_definition(&info.definition);
	}
}

void print_chunks(const std::string& file, pubsub::ArgParser& parser)
{
	rucksack::Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

    bool verbose = parser.GetBool("v");

	auto header = sack.get_header();

	// now iterate through each and every chunk
	char op_code;
	while (char* chunk_ptr = sack.read_block(op_code))
	{
		auto header = (rucksack::ChunkHeader*)chunk_ptr;
		if (op_code == rucksack::constants::ConnectionHeaderOp)
		{
			rucksack::ConnectionHeader* chunk = (rucksack::ConnectionHeader*)chunk_ptr;

			// read in the details about this topic/connection
			const char* topic = &chunk_ptr[sizeof(rucksack::ConnectionHeader)];

			printf("Chunk Type: Connection Header\n");
			printf("Chunk Lengh: %i bytes\n", header->length_bytes);
			printf("Connection ID: %i\n", chunk->connection_id);
			printf("Flags: %i\n", chunk->flags);
			printf("Topic: %s\n", topic);
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			rucksack::DataChunk* chunk = (rucksack::DataChunk*)chunk_ptr;

			printf("Chunk Type: Data\n");
			printf("Chunk Lengh: %i bytes\n", header->length_bytes);
			printf("Connection ID: %i\n", chunk->connection_id);
			printf("Start: %s\n", pubsub::Time(chunk->start_time).toString().c_str());
			printf("End: %s\n", pubsub::Time(chunk->end_time).toString().c_str());
		}
		else
		{
			printf("Chunk Type: Unknown\n");
			printf("Chunk Lengh: %i bytes\n", header->length_bytes);
		}
		printf("------------\n");
		delete[] chunk_ptr;
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

	if (parser.GetBool("c"))
	{
		print_chunks(file, parser);
		return;
	}

	bool verbose = parser.GetBool("v");
	int array_count = parser.GetDouble("a");

	rucksack::MessageHeader const* hdr;
	rucksack::SackChannelDetails const* def;
	while (const void* msg = sack.read(hdr, def))
	{
		printf("topic: %s\n---\n", def->topic.c_str());
		if (verbose)
		{
			printf("timestamp: %" PRIu64 ".%" PRIu64 "\n", hdr->time/1000000, hdr->time%1000000);
		}
		ps_deserialize_print(msg, &def->definition, array_count, 0);
		printf("------------\n");
	}

	return;
}

//then need to be able to play it back with accurate timing
//todo, sim time?

void play(const std::vector<std::string>& files, pubsub::ArgParser& parser)
{
	// todo handle multiple files
	std::string file = files[0];
	rucksack::Sack sack;
	if (!sack.open(file))
	{
		printf("ERROR: Opening sack failed!\n");
		return;
	}

	auto header = sack.get_header();

	ps_node_t node;
	ps_node_init(&node, "rucksack", "", true);

	struct ps_transport_t tcp_transport;
	ps_tcp_transport_init(&tcp_transport, &node);
	ps_node_add_transport(&node, &tcp_transport);

	struct ChannelOutput;
	struct MessageIndex
	{
		uint64_t time;
		ChannelOutput* channel;
		const char* ptr;
	};

	struct ChannelOutput
	{
		std::shared_ptr<char[]> topic;
		std::string type;
		std::shared_ptr<ps_message_definition_t> definition;
		ps_pub_t* publisher;

		bool latched;
		MessageIndex latched_message;

		void Release()
		{
			if (definition.get())
			{
				ps_free_message_definition(definition.get());
			}
			if (publisher)
			{
				delete publisher;
			}
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
	
	const bool loop = parser.GetBool("l");

	uint64_t last_time = 0;
	uint64_t first_time = std::numeric_limits<uint64_t>::max();
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
			details.latched_message.ptr = 0;
			details.latched_message.time = 0;
			details.latched = ((header->flags & rucksack::constants::CHFLAG_LATCHED) != 0);
			// create the publisher
			ps_node_create_publisher(&node, details.topic.get(), details.definition.get(), details.publisher, details.latched);
			channels[header->connection_id] = details;

			delete[] chunk_ptr;
		}
		else if (op_code == rucksack::constants::DataChunkOp)
		{
			rucksack::DataChunk* chunk = (rucksack::DataChunk*)chunk_ptr;

			if (chunk->connection_id >= channels.size())
			{
				printf("ERROR: Got data chunk with out-of-range channel id!");
				return;
			}

			first_time = std::min(chunk->start_time, first_time);
			last_time = std::max(chunk->end_time, last_time);

			chunks.push_back({ chunk_ptr, chunk_ptr + sizeof(rucksack::DataChunk) });
		}
		else
		{
			delete[] chunk_ptr;
		}

		// allow quick control-c
		if (ps_okay() == false)
		{
			return;
		}
	}

	const pubsub::Time start_time = pubsub::Time(first_time) + pubsub::Duration(req_start_time);
	const double length = (pubsub::Time(last_time) - start_time).toSec();

	if (req_start_time > length)
	{
		printf("Start time is past end of sackfile.\n");
		
		for (int i = 0; i < chunks.size(); i++)
		{
			delete[] chunks[i].block;
		}

		ps_node_destroy(&node);

    	// Free the channel infos
    	for (auto& info: channels)
    	{
        	info.Release();
    	}
		return;
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
				if (details->latched)
				{
					// its latched, keep track of the latest message less than start time
					if (idx.time >= details->latched_message.time)
					{
						details->latched_message = idx;
					}
				}

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

	// Publish any latched topics that happen before start time
	for (const auto& channel: channels)
	{
		if (channel.latched_message.ptr)
		{
			rucksack::MessageHeader* hdr = (rucksack::MessageHeader*)channel.latched_message.ptr;
			const char* data = channel.latched_message.ptr + sizeof(rucksack::MessageHeader);
			ps_msg_t msg;
			ps_msg_alloc(hdr->length, 0, &msg);
			memcpy(ps_get_msg_start(msg.data), data, hdr->length);
			ps_pub_publish(channel.publisher, &msg);
		}
	}

	// give the node a bit to advertise
	wait(&node);

	const double time_scale = parser.GetDouble("r");
	const double inv_time_scale = 1.0 / time_scale;
	const double inv_time_scale_ms = inv_time_scale / 1000.0;

	if (parser.GetBool("p"))
	{
		// start paused
		printf("\r [PAUSED]  Sack Time: %13.6f   Duration: %.6f / %.6f               \r",
				pubsub::Time(start_time).toSec(),
				(pubsub::Time(start_time) - start_time).toSec(),
				length);

		while (true)
		{
			// handle paused
			
			fflush(stdout);
			ps_node_spin(&node);
			ps_sleep(1);

			if ((_kbhit() && getc(stdin) == ' ') || ps_okay() == false)
			{
				break;
			}
		}
	}

	pubsub::Time real_begin = pubsub::Time::now();
	// now lets start publishing each message
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
			double dt = (diff - real_diff)*inv_time_scale_ms;

			// make sure we dont sleep too long so control c works if the bag is silly
			if (dt > 500)
			{
				dt = 500;

				if (ps_okay() == false)
				{
					return;
				}
			}
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
				fflush(stdout);
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

	auto chunk_size = parser.GetDouble("c");

	osack.create(out_file, start_time, chunk_size*1000);

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
	/*argc = 3;
	argv = new char* [3];
	argv[1] = "info";
	argv[2] = "recording.sack";*/

	std::string verb = argc > 1 ? argv[1] : "";

	pubsub::ArgParser parser;

	if (verb == "record")
	{
		parser.SetUsage("Usage: rucksack record TOPIC... [OPTION...]\n\nRecords topics to a file.\n");
		parser.AddFlag({ "a", "all" }, "Record all topics");
		parser.AddOption({ "c", "chunk-size" }, "Chunk size in kB", "768");
		parser.AddOption({ "d", "duration" }, "Length in seconds to record", "-1.0");
		parser.AddOption({ "n" }, "Number of messages to record", "-1");
		parser.AddOption({ "o" }, "Name of sack file", "");
		parser.AddOption({ "split-size", "ss" }, "Start recording to a new sack file if this file size in MB is exceeded. Set to zero to not split based on size.", "0");
		parser.AddOption({ "split-duration", "sd" }, "Start recording to a new sack file if this time period in seconds is exceeded. Set to zer to not split based on duration.", "0");

		parser.Parse(argv, argc, 1);

		record(parser);
	}
	else if (verb == "info")
	{
		parser.SetUsage("Usage: rucksack info FILE... [OPTION...]\n\nPrints information about rucksack files.\n");
		parser.AddFlag({ "v" }, "Print additional info");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		if (files.size() == 0)
		{
			printf("ERROR: Must provide at least one file.\n");
			return 1;
		}
		for (auto file : files)
		{
			info(file, parser);
		}
	}
	else if (verb == "play")
	{
		parser.SetUsage("Usage: rucksack play FILE... [OPTION...]\n\nPlays back topics stored in rucksack files.\n");
		parser.AddOption({ "r" }, "Rate to play the rucksack at.", "1.0");
		parser.AddOption({ "s" }, "Offset to start playing the rucksack at.", "0.0");
		parser.AddFlag({ "p" }, "Start playback paused.");
		parser.AddFlag({ "l", "loop" }, "If set, loop through the rucksack rather than stopping at the end.");

		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		if (files.size() == 0)
		{
			printf("ERROR: Must provide at least one file.\n");
			return 1;
		}
		play(files, parser);
	}
	else if (verb == "print")
	{
		parser.SetUsage("Usage: rucksack print FILE...\n\nPrints each message stored in rucksack files.");
		parser.AddFlag({ "v" }, "Print timestamps along with each message.");
		parser.AddOption({ "a" }, "Maximum number of array elements to print. Set to 0 for unlimited.", "20");
		parser.AddFlag({ "c" }, "Print chunks instead of messages.");
		
		parser.Parse(argv, argc, 1);

		auto files = parser.GetAllPositional();
		if (files.size() == 0)
		{
			printf("ERROR: Must provide at least one file.\n");
			return 1;
		}
		for (auto file : files)
		{
			print(file, parser);
		}
	}
	else if (verb == "merge")
	{
		// merges two sack files into one
		parser.SetUsage("Usage: rucksack merge FILE... -o OUTPUT_FILE\n\nCombines the contents of multiple sack files into one.");
		parser.AddOption({ "o", "output" }, "Name for output combined sack file.", "");
		parser.AddOption({ "c", "chunk-size" }, "Chunk size to use for output sack file in kB.", "768");
		parser.Parse(argv, argc, 1);

		merge(parser.GetAllPositional(), parser);
	}
	else if (verb == "migrate")
	{
		parser.SetUsage("Usage: rucksack migrate FILE...\n\nMigrates the given rucksack file to the latest format.");
		parser.AddFlag({"d"}, "Delete migrated sackfiles");
		parser.Parse(argv, argc, 1);

		// migrate to the current bag version
		auto files = parser.GetAllPositional();
		if (files.size() == 0)
		{
			printf("ERROR: Must provide at least one file.\n");
			return 1;
		}
		for (auto file : files)
		{
			std::string copy = file + ".new";
			printf("Migrating '%s'...\n", file.c_str());

			bool success = rucksack::SackMigrator::Migrate(copy, file);

			// then delete if wanted + successful
			if (success && parser.GetBool("d"))
			{
				// todo
			}
			else if (!success)
			{
				printf("Migration failed\n");
			}
		}
	}
    else
    {
        if (verb.length())
        {
            printf("Unknown Verb: %s\n", verb.c_str());
        }

        // give top level help
        print_help();
    }
	return 0;
}
