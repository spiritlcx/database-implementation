#include <list>
#include <unordered_map>

//2Q replacement strategy is used
class BufferReplacement{
public:
	uint64_t remove();
	void add(uint64_t pageId);
	void reference(uint64_t pageId);
private:
//indicate whether a pageId is in fifoQueue or in lruQueue
	std::unordered_map<uint64_t, bool> fifoMap;
	std::list<uint64_t> fifoQueue;
	std::list<uint64_t> lruQueue;
};
