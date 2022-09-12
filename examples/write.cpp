#include <rucksack/rucksack.h>

#include <pubsub/Node.h>

#include <pubsub/String.msg.h>

int main()
{
  // Create the file
  rucksack::SackWriter sack;
  sack.create("writing_test.sack", pubsub::Time(0), 10000);

  // Create a message to write
  pubsub::msg::String string;
  string.value = "testing";

  // Write the message a lot of times
  for (int i = 0; i < 50000; i++)
  {
    sack.write_message("/test", string, pubsub::Time(i/1000, (i%1000)*1000));
  }

  string.value = 0;

  // finish
  sack.close();

  return 0;
}
