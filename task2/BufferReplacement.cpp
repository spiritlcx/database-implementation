#include "BufferReplacement.hpp"

uint64_t BufferReplacement::remove(){
	uint64_t toRemove;
	if(fifoQueue.size() != 0){
		toRemove = fifoQueue.front();
		fifoQueue.pop_front();
		fifoMap.erase(toRemove);
	}else{
		toRemove = lruQueue.front();
		lruQueue.pop_front();
	}
	return toRemove;
}

void BufferReplacement::add(uint64_t pageId){
	fifoQueue.push_back(pageId);
	fifoMap.insert({pageId, true});
}

void BufferReplacement::reference(uint64_t pageId){
	if(fifoMap.find(pageId) != fifoMap.end()){
		fifoQueue.remove(pageId);
		fifoMap.erase(pageId);
	}else{
		lruQueue.remove(pageId);
	}   
	lruQueue.push_back(pageId);
}
