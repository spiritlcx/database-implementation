#include "Basic.hpp"
#include "SSegment.hpp"
#include "Record.hpp"
#include "TID.hpp"

class SPSegment{
public:
	SPSegment(BufferManager *bufferManager, SSegment *sSegment, std::string tableName);
	TID insert(const Record& r, uint64_t page_ignore = UINTMAX_MAX);
	bool remove(TID tid);
	Record lookup(TID tid);
	bool update(TID tid, const Record& r);
	uint64_t getSlotCount(uint64_t pageId);
private:
//STID consists of segmentId and pageId and is used in BufferManager
//TID consists of pageId and slotId
	uint64_t STID(uint64_t pageId);
	uint16_t segmentId;
	uint64_t numPages;
	SSegment *sSegment;
	BufferManager *bufferManager;
//Informatin of the relation whose segmentId is segmentId
	Schema::Relation *relation;
};
