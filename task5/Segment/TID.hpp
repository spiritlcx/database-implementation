#ifndef HPP_TID
#define HPP_TID

//consists of pageId and slotId
class TID{
public:
	TID(uint64_t pageId, uint16_t slotId){
		tid = (pageId << 16) | slotId;
	}
	TID(uint64_t tid){
		this->tid = tid;
	}
	TID(){tid = 0;}

	TID &operator = (const TID& other){
		this->tid = other.tid;
		return *this;
	}
	uint64_t getValue(){
		return tid;
	}
	uint64_t getPageId(){
		return tid >> 16;
	}
	uint16_t getSlotId(){
		return tid;
	}
	uint64_t tid;
};

#endif
