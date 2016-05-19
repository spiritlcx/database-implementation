#include "BufferManager.hpp"
#include <iostream>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <utility>
BufferManager::BufferManager(unsigned pageCount){
	if(pageCount <= 0){
		throw new InvalidPageNumber();
	}
	this->pageCount = pageCount;
	pthread_mutex_init(&lock ,NULL);
}

BufferFrame& BufferManager::find(uint64_t actPageId){
	BufferFrame &frame = frames[actPageId];
	return frame;
}

//check if the buffer is full, if yes, remove a page
void BufferManager::check_remove_insert(uint64_t actPageId){
	if(frames.size() > pageCount){
		uint64_t pageToRemove = replacement.remove();
		BufferFrame &frame = frames[pageToRemove];
//waiting until all threads release locks on this frame
		while(frame.number != 0);
			
		if(frame.state == State::DIRTY)
			flush(frame);

		frames.erase(pageToRemove);
	}
	replacement.add(actPageId);
}

BufferFrame& BufferManager::fixPage(uint64_t pageId, bool exclusive){
	SegmentPage segmentPage = extractId(pageId);
	uint32_t segmentId = segmentPage.segmentId;
	uint64_t actPageId = segmentPage.pageId;

//The hashMap is locked when being searched or modified
	pthread_mutex_lock(&lock);
	BufferFrame &frame = find(actPageId);
	if(frame.pageId == actPageId){
		if(exclusive){
			if(frame.wlock() != 0)
				std::cout << "cannot acquire the write lock" <<std::endl;
			replacement.reference(actPageId);
			pthread_mutex_unlock(&lock);
			return frame;
		}else{
			if(frame.rlock() != 0){
				std::cout << "cannot acquire the read lock" <<std::endl;
			}
			replacement.reference(actPageId);
			pthread_mutex_unlock(&lock);
			return frame;
		}
	}else{
		if(segments.find(segmentId) == segments.end()){
			segments[segmentId] = open(std::to_string(segmentId).c_str(), O_CREAT|O_RDWR, S_IWUSR|S_IRUSR);
		}

		if(segments[segmentId] == -1){
			std::cerr << strerror(errno) << std::endl;
			throw new SegmentNotExist();
		}

		check_remove_insert(actPageId);
//write lock is acquired the first time a page is loaded in to memory	
		if(frame.wlock() != 0){
			std::cout << "cannot acquire the write lock" <<std::endl;
		}

		frame.pageId = actPageId;
		frame.state = State::NEWLY;
		pthread_mutex_unlock(&lock);
				
		pread(segments[segmentId], frame.getData(), PAGESIZE, actPageId*PAGESIZE);
		return frame;
	}
}

void BufferManager::unfixPage(BufferFrame& frame, bool isDirty){

	if(isDirty)
		frame.set(State::DIRTY);
	frame.unlock();	
}

BufferManager::~BufferManager(){
//all dirty pages are written to disk 
	for(auto it = frames.begin(); it != frames.end(); it++){
		BufferFrame &frame = it->second;
		if(frame.state == State::DIRTY){
			flush(frame);
		}
	}
//close all segments
	for(auto it = segments.begin(); it != segments.end(); it++)
		close(it->second);

	pthread_mutex_destroy(&lock);
}

//write a page to disk
void BufferManager::flush(BufferFrame &frame){
	SegmentPage segmentPage = extractId(frame.pageId);
	uint32_t segmentId = segmentPage.segmentId;
	uint64_t actPageId = segmentPage.pageId;
	pwrite(segments[segmentId], frame.getData(), PAGESIZE, actPageId*PAGESIZE);
}

SegmentPage BufferManager::extractId(uint64_t pageId){
	SegmentPage segmentPage;
	segmentPage.segmentId = pageId >> PAGEBIT;
	segmentPage.pageId = (pageId << SEGMENTBIT) >> SEGMENTBIT;
	return segmentPage;
}
