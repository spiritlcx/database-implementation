#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <cassert>
#include <string.h>

#include "Segment/SPSegment.hpp" // include your stuff here
#include "Segment/Record.hpp"

using namespace std;

// todo: adapt to your implementation
uint64_t extractPage(TID tid) {
   return tid.getPageId();
}

const unsigned initialSize = 100; // in (slotted) pages
const unsigned totalSize = initialSize+50; // in (slotted) pages
const unsigned maxInserts = 1000ul*1000ul;
const unsigned maxDeletes = 10ul*1000ul;
const unsigned maxUpdates = 10ul*1000ul;
const double loadFactor = .7; // percentage of a page that can be used to store the payload
const vector<string> testData = {
   "640K ought to be enough for anybody",
   "Beware of bugs in the above code; I have only proved it correct, not tried it",
   "Tape is Dead. Disk is Tape. Flash is Disk.",
   "for seminal contributions to database and transaction processing research and technical leadership in system implementation",
   "Stephen Curry heats up for 28 points as the Warriors beat the Thunder in Game 2"};
           
class Random64 {
   uint64_t state;
   public:
   explicit Random64(uint64_t seed=88172645463325252ull) : state(seed) {}
   uint64_t next() {
      state^=(state<<13); state^=(state>>7); return (state^=(state<<17));
   }
};

int main(int argc, char** argv) {
   // Check arguments
   if (argc != 2) {
      cerr << "usage: " << argv[0] << " <pageSize>" << endl;
      return -1;
   }
   const unsigned pageSize = atoi(argv[1]);

   // Bookkeeping
   unordered_map<uint64_t, unsigned> values; // TID -> testData entry
   unordered_map<unsigned, unsigned> usage; // pageID -> bytes used within this page

   // Setting everything
   BufferManager bm(100);

   SSegment sSegment("test.sql");
   //segmentId is 1
   SPSegment sp(&bm, &sSegment, 1);
   Random64 rnd;

   // Insert some records
   for (unsigned i=0; i<maxInserts; ++i) {
      // Select string/record to insert
      uint64_t r = rnd.next()%testData.size();
      const string s = testData[r];

      // Check that there is space available for 's'
      bool full = true;
      for (unsigned p=0; p<initialSize; ++p) {
         if (usage[p] < loadFactor*pageSize) {
            full = false;
            break;
         }
      }
      if (full)
         break;

      // Insert record
      TID tid = sp.insert(Record(s.size(), s.c_str()));
      assert(values.find(tid.getValue())==values.end()); // TIDs should not be overwritten
      values[tid.getValue()]=r;
      unsigned pageId = extractPage(tid); // extract the pageId from the TID
	  assert(pageId < initialSize); // pageId should be within [0, initialSize)
      usage[pageId]+=s.size();
   }
	std::cout << "insert finished"<<std::endl;

   // Lookup & delete some records
   for (unsigned i=0; i<maxDeletes; ++i) {
      // Select operation
      bool del = rnd.next()%10 == 0;
	del = false;
      // Select victim
      TID tid = values.begin()->first;
      unsigned pageId = extractPage(tid);
      const std::string& value = testData[(values.begin()->second)%testData.size()];
      unsigned len = value.size();

      // Lookup
      Record rec = sp.lookup(tid);
      assert(rec.getLen() == len);
      assert(memcmp(rec.getData(), value.c_str(), len)==0);
      if (del) { // do delete
         assert(sp.remove(tid));
        values.erase(tid.getValue());
         usage[pageId]-=len;
      }
   }
	std::cout << "delete finished"<<std::endl;
   // Update some values ('usage' counter invalid from here on)
   for (unsigned i=0; i<maxUpdates; ++i) {
      // Select victim
      TID tid = values.begin()->first;

      // Select new string/record
      uint64_t r = rnd.next()%testData.size();
      const string s = testData[r];
      // Replace old with new value
      sp.update(tid, Record(s.size(), s.c_str()));

	  values[tid.getValue()]=r;
   }
	std::cout << "update finished"<<std::endl;
   // Lookups

   for (auto p : values) {
      TID tid = p.first;
      const std::string& value = testData[p.second];
      unsigned len = value.size();
      Record rec = sp.lookup(tid);
      assert(rec.getLen() == len);
      assert(memcmp(rec.getData(), value.c_str(), len)==0);
   }
	std::cout << "All tests passed" <<std::endl;
   return 0;
}
