#include "SlottedPage.hpp"

void SlottedPage::initialize(){
	header.LSN = 0;
	header.slotCount = 0;
	header.spaceLeft = PAGESIZE - sizeof(header);
	header.firstFreeSlot = 0;
	header.dataStart = PAGESIZE - 1 - sizeof(Header);
	header.freeSpace = header.spaceLeft;
}

unsigned SlottedPage::getSpace(){
	return header.spaceLeft;
}

bool SlottedPage::enoughSpace(uint16_t length){
	if((length + sizeof(Slot)) <= header.spaceLeft)
		return true;
	return false;
}

Data SlottedPage::getData(uint16_t slotId){
	Data data;
	Slot &slot = slots[slotId];

	if(slot.position.tid == UINTMAX_MAX){
		data.data = records+slot.offset;
		data.tid.tid = UINTMAX_MAX;
	}else{
		data.tid = slot.position;
		data.data = nullptr;
	}
	return data;
}

uint16_t SlottedPage::getLength(uint16_t slotId){
	return slots[slotId].length;
}

uint16_t SlottedPage::insert(unsigned len, const char* data){
	if(!enoughSpace(len)){
		return -1;
	}
	Slot slot;

	slot.offset = header.dataStart - len + 1;
	slot.length = len;
	slots[header.firstFreeSlot] = slot;
	memcpy(records + slot.offset, data, len);

	header.slotCount++;
	header.firstFreeSlot++;
	header.spaceLeft -= (len + sizeof(Slot));
	header.freeSpace -= (len + sizeof(Slot));
	header.dataStart = slot.offset - 1;

	return header.firstFreeSlot - 1;
}

bool SlottedPage::remove(uint16_t slotId){
	if(slotId >= header.slotCount){
		return false;
	}
	slots[slotId].offset = 0;
//check if it's the last slot
	if(slotId == header.slotCount-1){
		header.slotCount--;
		header.firstFreeSlot--;
		header.dataStart += slots[slotId].length;
		header.spaceLeft += (slots[slotId].length + sizeof(Slot));
	}
	
	header.freeSpace += (slots[slotId].length + sizeof(Slot));

	return true;
}

bool SlottedPage::update(uint16_t slotId, uint16_t len, const char* data){

//if the length of new data is same as or smaller than old data, then update in place
//if the length is bigger than old data, but can be contained in this page, insert the new data to front
//otherwise, make an indirection to another page(done in SPSegment::update)
	Slot &slot = slots[slotId];
	if(len <= slot.length){
		memcpy(records + slot.offset, data, len);
		slot.length = len;
		header.freeSpace += (slot.length - len);
		return true;
	}else{
		if(getSpace() >= len){
			uint16_t offset = header.dataStart - len + 1;
			memcpy(records + offset, data, len);
			header.spaceLeft -= len;
			header.freeSpace += slot.length;
			header.freeSpace -= len;
			slot.offset = offset;
			slot.length = len;
			return true;
		}
	}
		
	return false;
}

void SlottedPage::compact(){
	uint16_t start = PAGESIZE - 1 - sizeof(Header);
	uint16_t cumulative = 0;
	uint16_t initSlotCount = header.slotCount;
	for(int i = 0; i < initSlotCount; i++){
		if(!slots[i].empty()){
			//needs to be moved only when it is not in the right position
			if(slots[i].offset + slots[i].length != cumulative){
				memcpy(records + start - slots[i].length + 1, records + slots[i].offset, slots[i].length);
				start -= slots[i].length;
			}	
			cumulative += slots[i].length;
		}else{
			//next slot needs to be moved to this position, and next slot is set to be empty
			slots[i] = slots[i+1];
			slots[i+1].offset = 0;
			header.spaceLeft += (slots[i+1].length + sizeof(Slot));
			header.slotCount--;
			i--;
		}
	}
	header.firstFreeSlot = header.slotCount;
	header.dataStart = start;
	header.freeSpace = header.spaceLeft;
}

void SlottedPage::redirect(uint16_t slotId, TID tid){
	Slot slot;
	slot.position = tid;
	slot.offset = 0;
	slots[slotId] = slot;
}

