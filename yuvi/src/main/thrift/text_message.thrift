// TextMessage class takes in a list of OpenTSDB metrics as input.

namespace java com.pinterest.yuvi.thrift

struct TextMessage {
  1: required list<string> messages;
  2: optional string host;
  3: optional string filename;
}
