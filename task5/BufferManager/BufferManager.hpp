#include "BufferFrame.hpp"
#include "BufferReplacement.hpp"
#include <unordered_map>
#include <exception>

constexpr unsigned PAGEBIT = 48;
constexpr unsigned SEGMENTBIT = 64 - PAGEBIT;

struct SegmentPage{
	uint32_t segmentId;
	uint64_t pageId;
};

class BufferManager{
public:
	BufferManager(unsigned pageCount);
	BufferFrame& fixPage(uint64_t pageId, bool exclusive);
	void unfixPage(BufferFrame& frame, bool isDirty);
	void upgrade(uint64_t pageId);
	~BufferManager();
private:
	void flush(BufferFrame &frame);
	std::unordered_map<uint64_t, BufferFrame> frames;
	std::unordered_map<uint32_t, int> segments;
	BufferFrame& find(uint64_t pageId);
	void check_remove_insert(uint64_t actPageId);
	SegmentPage extractId(uint64_t pageId);
//upgrade the priority

	unsigned pageCount;
	pthread_mutex_t lock;
	BufferReplacement replacement;
};

class SegmentNotExist : public std::exception{
	virtual const char* what() const throw(){
		return "Segment not exist exception is thrown";
	}
};

class InvalidPageNumber : public std::exception{
	virtual const char* what() const throw(){
		return "Page number should be more than 0";
	}
};
