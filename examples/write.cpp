#include <rucksack/rucksack.h>

#include <pubsub/Node.h>

#include <pubsub/std_msgs__String.msg.h>

int main()
{
  // make a message to write to the bag file
  rucksack::SackWriter sack;
  sack.create("writing_test.sack", pubsub::Time(0), 1000);

  std_msgs::String string;
  string.value = "testing";

  // write it n times
  for (int i = 0; i < 50000; i++)
  {
    sack.write_message("/test", string);
  }

  string.value = 0;

  // finish
  sack.close();

  return 0;
}
