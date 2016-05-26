#include "../BufferManager/BufferManager.hpp"

template<typename T>
struct Element{
	uint64_t pageId;
	T key;
};

template<typename T>
class InterNode{
public:

private:
	Element<T> elements[(PAGESIZE-sizeof(uint64_t))/sizeof(Element<T>)];
	uint64_t lastPageId;
};
