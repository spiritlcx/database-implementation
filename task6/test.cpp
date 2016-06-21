#include <iostream>
#include <cstdlib>
#include <atomic>
#include <string.h>
#include <tbb/tbb.h>
#include <tbb/mutex.h>
#include <unordered_map>
#include <vector>
using namespace tbb;
using namespace std;

inline uint64_t hashKey(uint64_t k) {
	// MurmurHash64A
	const uint64_t m = 0xc6a4a7935bd1e995;
	const int r = 47;
	uint64_t h = 0x8445d61a4e774912 ^ (8*m);
	k *= m;
	k ^= k >> r;
	k *= m;
	h ^= k;
	h *= m;
	h ^= h >> r;
	h *= m;
	h ^= h >> r;
	return h|(1ull<<63);
}

class ChainingLockingHT {
public:
	// Chained tuple entry
	struct Entry {
		uint64_t key;
		uint64_t value;
		Entry* next=nullptr;

	};

	// Constructor
	ChainingLockingHT(uint64_t size) {
		entries = new Entry*[size];
		memset(entries, 0, size*sizeof(Entry*));
		mutexes = new tbb::mutex[size];
		this->size = size;
	}

	// Destructor
	~ChainingLockingHT() {
//		for(int i = 0; i < size; i++)
//			delete entries[i];
//		delete [] entries;
	}

	// Returns the number of hits
	inline uint32_t lookup(uint64_t key) {
		uint64_t bucket = key%size;
		Entry *current = entries[bucket];

		uint32_t count = 0;
		while(current != nullptr){
			count += (current->key == key);
			current = current->next;
		}
		return count;
	}

	inline void insert(Entry* entry) {
		uint64_t bucket = entry->key%size;


		mutexes[bucket].lock();
		entry->next = entries[bucket];
		entries[bucket] = entry;

		mutexes[bucket].unlock();

	}
private:
	Entry* *entries;
	tbb::mutex *mutexes;
	int size;
};

class ChainingHT {
public:
	// Chained tuple entry
	struct Entry {
		uint64_t key;
		uint64_t value;
		Entry* next=nullptr;
	};

	// Constructor
	ChainingHT(uint64_t size) {
		entries = new std::atomic<Entry*>[size];
		memset(entries, 0, size*sizeof(Entry*));
		this->size = size;
	}

	// Destructor
	~ChainingHT() {
//		for(int i = 0; i < size; i++)
//			delete entries[i];
//		delete [] entries;
	}

	// Returns the number of hits
	inline uint32_t lookup(uint64_t key) {
		uint64_t bucket = key%size;
		Entry* current = entries[bucket].load(std::memory_order_relaxed);

		uint32_t count = 0;
		while(current != nullptr){
			count += (current->key == key);
			current = current->next;
		}
		return count;
	}

	inline void insert(Entry* entry) {
		uint64_t bucket = entry->key%size;
		Entry *current = entries[bucket].load(std::memory_order_relaxed);
		entry->next = current;

		while(true){
			if(entries[bucket].compare_exchange_weak(current, entry, std::memory_order_release, std::memory_order_relaxed)){
				break;
			}else{
				current = entries[bucket].load(std::memory_order_relaxed);
				entry->next = current;
			}
		}
	}
private:
	std::atomic<Entry*>* entries;
	int size;
};

/*
   class LinearProbingHT {
   public:
// Entry
struct Entry {
uint64_t key;
uint64_t value;
std::atomic<bool> marker;
};

// Constructor
LinearProbingHT(uint64_t size) {
entries = new std::atomic<Entry>[4*size];
this->size = 4*size;
}

// Destructor
~LinearProbingHT() {

}
// Returns the number of hits
inline uint64_t lookup(uint64_t key) {
uint64_t bucket = hashKey(key)%size;
uint64_t count = 0;
while(true){
Entry entry = entries[bucket].load(std::memory_order_relaxed);
if(entry.marker == false)
break;
count += (entry.key == key);
bucket++;
if(bucket == size)
bucket = 0;
}
return count;
}
inline void insert(uint64_t key, uint64_t value) {
uint64_t bucket = hashKey(key)%size;

while(true){
while(true){
Entry entry = entries[bucket].load(std::memory_order_relaxed);
if(entry.marker == false)
break;
bucket++;
if(bucket == size)
bucket = 0;
}

Entry entry;
entry.key = key;
entry.value = value;
entry.marker = true;

Entry initial;
initial.marker = false;

if(entries[bucket].compare_exchange_weak(initial, entry, std::memory_order_release, std::memory_order_relaxed)){
break;
}
}
}

private:
std::atomic<Entry> *entries;
uint64_t size;
};

 */
int main(int argc,char** argv) {
	uint64_t sizeR = atoi(argv[1]);
	uint64_t sizeS = atoi(argv[2]);
	unsigned threadCount = atoi(argv[3]);

	task_scheduler_init init(threadCount);

	// Init build-side relation R with random data
	uint64_t* R=static_cast<uint64_t*>(malloc(sizeR*sizeof(uint64_t)));
	parallel_for(blocked_range<size_t>(0, sizeR), [&](const blocked_range<size_t>& range) {
			unsigned int seed=range.begin();
			for (size_t i=range.begin(); i!=range.end(); ++i)
			R[i]=rand_r(&seed)%sizeR;
	});

	// Init probe-side relation S with random data
	uint64_t* S=static_cast<uint64_t*>(malloc(sizeS*sizeof(uint64_t)));
	parallel_for(blocked_range<size_t>(0, sizeS), [&](const blocked_range<size_t>& range) {
			unsigned int seed=range.begin();
			for (size_t i=range.begin(); i!=range.end(); ++i)
			S[i]=rand_r(&seed)%sizeR;
	});

	// STL
	{
		// Build hash table (single threaded)
		tick_count buildTS=tick_count::now();
		unordered_multimap<uint64_t,uint64_t> ht(sizeR);
		for (uint64_t i=0; i<sizeR; i++)
			ht.emplace(R[i],0);
		tick_count probeTS=tick_count::now();
		cout << "STL             build:" << (sizeR/1e6)/(probeTS-buildTS).seconds() << "MT/s ";

		// Probe hash table and count number of hits
		std::atomic<uint64_t> hitCounter;
		hitCounter=0;
		parallel_for(blocked_range<size_t>(0, sizeS), [&](const blocked_range<size_t>& range) {
				uint64_t localHitCounter=0;
				for (size_t i=range.begin(); i!=range.end(); ++i) {
					auto range=ht.equal_range(S[i]);
					for (unordered_multimap<uint64_t,uint64_t>::iterator it=range.first; it!=range.second; ++it)
						localHitCounter++;
				}
				hitCounter+=localHitCounter;
		});
		tick_count stopTS=tick_count::now();
		cout << "probe: " << (sizeS/1e6)/(stopTS-probeTS).seconds() << "MT/s "
			<< "total: " << ((sizeR+sizeS)/1e6)/(stopTS-buildTS).seconds() << "MT/s "
			<< "count: " << hitCounter << endl;
	}

	// Test you implementation here... (like the STL test above)
	{
		tick_count buildTS=tick_count::now();
		ChainingLockingHT ht(sizeR);

		parallel_for(blocked_range<size_t>(0, sizeR), [&](const blocked_range<size_t>& range) {
				for(uint64_t i = range.begin(); i != range.end(); i++){
					ChainingLockingHT::Entry *entry = new ChainingLockingHT::Entry();
					entry->key = R[i];
					entry->value = 0;
					ht.insert(entry);
				}
		});


		tick_count probeTS=tick_count::now();
		cout << "ChainingLocking build:" << (sizeR/1e6)/(probeTS-buildTS).seconds() << "MT/s ";
		std::atomic<uint64_t> hitCounter;
		hitCounter = 0;
		parallel_for(blocked_range<size_t>(0, sizeS), [&](const blocked_range<size_t>& range) {
				uint32_t localCounter = 0; 

				for(size_t i = range.begin(); i != range.end(); i++){
					localCounter += ht.lookup(S[i]);
				}
				hitCounter += localCounter;
		});

		tick_count stopTS=tick_count::now();
		cout << "probe: " << (sizeS/1e6)/(stopTS-probeTS).seconds() << "MT/s "
			<< "total: " << ((sizeR+sizeS)/1e6)/(stopTS-buildTS).seconds() << "MT/s "
			<< "count: " << hitCounter << endl;


	}

	{
		tick_count buildTS=tick_count::now();
		ChainingHT ht(sizeR);

		parallel_for(blocked_range<size_t>(0, sizeR), [&](const blocked_range<size_t>& range) {
				for(uint64_t i = range.begin(); i != range.end(); i++){
					ChainingHT::Entry *entry = new ChainingHT::Entry();
					entry->key = R[i];
					entry->value = 0;
					ht.insert(entry);
				}
		});


		tick_count probeTS=tick_count::now();
		cout << "Chaining        build:" << (sizeR/1e6)/(probeTS-buildTS).seconds() << "MT/s ";
		std::atomic<uint32_t> hitCounter;
		hitCounter = 0;
		parallel_for(blocked_range<size_t>(0, sizeS), [&](const blocked_range<size_t>& range) { 
				uint32_t localCounter = 0;
				for(size_t i = range.begin(); i != range.end(); i++){
					localCounter += ht.lookup(S[i]);
				}
				hitCounter += localCounter;
		});

		tick_count stopTS=tick_count::now();
		cout << "probe: " << (sizeS/1e6)/(stopTS-probeTS).seconds() << "MT/s "
			<< "total: " << ((sizeR+sizeS)/1e6)/(stopTS-buildTS).seconds() << "MT/s "
			<< "count: " << hitCounter << endl;


	}

	/* 
	   {
	   tick_count buildTS=tick_count::now();
	   LinearProbingHT ht(sizeR);

	   parallel_for(blocked_range<size_t>(0, sizeR), [&](const blocked_range<size_t>& range) {
	   for(uint64_t i = range.begin(); i != range.end(); i++){
	   LinearProbingHT::Entry *entry = new LinearProbingHT::Entry();
	   ht.insert(R[i],0);
	   }
	   });


	   tick_count probeTS=tick_count::now();
	   cout << "STL build:" << (sizeR/1e6)/(probeTS-buildTS).seconds() << "MT/s ";
	   std::atomic<uint64_t> hitCounter;
	   hitCounter = 0;
	   parallel_for(blocked_range<size_t>(0, sizeS), [&](const blocked_range<size_t>& range) {


	   for(size_t i = range.begin(); i != range.end(); i++){
	   hitCounter += ht.lookup(S[i]);
	   }
	   });

	   tick_count stopTS=tick_count::now();
	   cout << "probe: " << (sizeS/1e6)/(stopTS-probeTS).seconds() << "MT/s "
	   << "total: " << ((sizeR+sizeS)/1e6)/(stopTS-buildTS).seconds() << "MT/s "
	   << "count: " << hitCounter << endl;


	   }
	 */

	return 0;
}
