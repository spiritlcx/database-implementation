#include <iostream>
#include <cstdint>
#include <pthread.h>
#include <string.h>
#include <atomic>

enum class State {CLEAN, DIRTY, NEWLY};
constexpr uint32_t PAGESIZE = 8000;
using byte = unsigned char;

class BufferFrame{
public:
	BufferFrame(){
		pthread_rwlock_init(&latch, NULL);
		state = State::CLEAN;
		number = 0;
		pageId = -1;
		data = new byte[PAGESIZE/sizeof(byte)];
	}
	~BufferFrame(){
		delete [] reinterpret_cast<byte*>(data);
		pthread_rwlock_destroy(&latch);
	}
	void* getData(){
		return data;
	}
	void set(State state){
		this->state = state;
	}
	int wlock(){
		number++;
		return pthread_rwlock_wrlock(&latch);
	}
	void unlock(){
		pthread_rwlock_unlock(&latch);
		number--;

	}
	int rlock(){
		number++;
		return pthread_rwlock_rdlock(&latch);
	}

	volatile std::atomic<int> number;
	uint64_t pageId;
	uint64_t LSN;
	void* data;
	State state;
private:	
	pthread_rwlock_t latch;
};
