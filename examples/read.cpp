
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
  ps_message_definition_t const* def;
  while (const void* msg = sack.read(hdr, def))
  {
    if (def->hash == pubsub::msg::String::GetDefinition()->hash)
    {
      // now we can deserialize it to the correct type
      pubsub::msg::String* str = pubsub::msg::String::Decode(msg);

 	  printf("Got string %s\n", str->value);
      printf("------------\n");
      delete str;// don't forget to free it
    }
    else
    {
      printf("Unhandled message type %s.\n", def->name);
    }
  }

  return 0;
}
