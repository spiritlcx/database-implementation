#include <vector>
#include <cstring>
#include <climits>
#include "../BufferManager/BufferFrame.hpp"
#include "TID.hpp"

struct Header{
//TODO 6 bytes are wasted
	uint64_t LSN;
//number of slots
	uint16_t slotCount;
//size of continuous space
	uint16_t spaceLeft;
//to speed up looking for free slot
	uint16_t firstFreeSlot;
//start of records, data is from right to left
	uint16_t dataStart;
//size after compaction
	uint16_t freeSpace;
};

struct Slot{
//TODO 4 bytes are wasted
	//point to a slot in another page
	TID position;
	uint16_t short offset;
	uint16_t short length;
	Slot(){
		//UINTMAX_MAX means it is in the current page
		//Otherwise, it is redirected to another page
		position.tid = UINTMAX_MAX; 
		offset = 0;
		length = 0;
	}

	bool empty(){
		return offset == 0;
	}
};

struct Data{
//if the data is not in current page, data will be null and a new tid will be returned
//otherwise tid is UINT_MAX
	TID tid;
	char *data;
};

class SlottedPage{
public:
//should be called the first time a page is created
	void initialize();
//space left	
	unsigned getSpace();
//whether there is enough space to store data whose length is length
	bool enoughSpace(uint16_t length);

	Data getData(uint16_t slotId);

	uint16_t getLength(uint16_t slotId);

	uint16_t insert(unsigned len, const char* data);

	bool update(uint16_t slotId, uint16_t len, const char* data);

	bool remove(uint16_t slotId);
//make a indirection to another page
	void redirect(uint16_t slotId, TID tid);
//TODO needs to be changed
	void compact();

private:
	Header header;
	union{
		Slot slots[(PAGESIZE-sizeof(Header))/sizeof(Slot)];
		char records[PAGESIZE-sizeof(Header)];
	};
};
