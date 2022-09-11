
#include <rucksack/rucksack.h>

#include <pubsub/String.msg.h>

// Takes in a bag file made by the write example and parses out each message
int main()
{
  rucksack::SackReader sack;
  if (!sack.open("writing_test.sack"))
  {
    printf("ERROR: Opening sack failed!\n");
    return 0;
  }

  rucksack::MessageHeader const* hdr;
  rucksack::SackChannelDetails const* info;
  while (const void* msg = sack.read(hdr, info))
  {
    if (info->definition.hash == pubsub::msg::String::GetDefinition()->hash)
    {
      // now we can deserialize it to the correct type
      pubsub::msg::String* str = pubsub::msg::String::Decode(msg);

 	  printf("Got string %s\n", str->value);
      printf("------------\n");
      delete str;// don't forget to free it
    }
    else
    {
      printf("Unhandled message type %s.\n", info->definition.name);
    }
  }

  return 0;
}
